import os
import sys
import threading
import json
import uuid
import time
import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from flask import Flask, jsonify
from dotenv import load_dotenv
from opentelemetry import metrics, propagate, trace
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind, Status, StatusCode

from telemetry import init_telemetry

# Configura o logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# Carrega .env para desenvolvimento local
load_dotenv()

# --- Configuração ---
AWS_REGION = os.getenv("AWS_REGION")
SQS_QUEUE_URL = os.getenv("AWS_SQS_URL")
DYNAMODB_TABLE_NAME = os.getenv("AWS_DYNAMODB_TABLE")
SQS_ENDPOINT = os.getenv("SQS_ENDPOINT")
DYNAMODB_ENDPOINT = os.getenv("DYNAMODB_ENDPOINT")


def validate_config():
    """Valida as configurações obrigatórias antes de iniciar o serviço."""
    if not all([AWS_REGION, SQS_QUEUE_URL, DYNAMODB_TABLE_NAME]):
        log.critical("Erro: AWS_REGION, AWS_SQS_URL, e AWS_DYNAMODB_TABLE devem ser definidos.")
        sys.exit(1)


validate_config()

app = Flask(__name__)
init_telemetry(app, default_service_name="analytics-service")

tracer = trace.get_tracer("togglemaster/analytics-service")
meter = metrics.get_meter("togglemaster/analytics-service")
message_counter = meter.create_counter(
    "togglemaster.analytics.messages",
    unit="1",
    description="Mensagens de avaliação processadas pelo analytics-service",
)
processing_duration = meter.create_histogram(
    "togglemaster.analytics.processing.duration",
    unit="ms",
    description="Tempo de processamento de cada mensagem SQS",
)


# --- Clientes Boto3 ---
def create_aws_clients():
    """Cria os clientes SQS e DynamoDB utilizados pelo worker."""
    try:
        session = boto3.Session(region_name=AWS_REGION)
        sqs = session.client("sqs", endpoint_url=SQS_ENDPOINT)
        dynamodb = session.client("dynamodb", endpoint_url=DYNAMODB_ENDPOINT)
        log.info(f"Clientes Boto3 inicializados na região {AWS_REGION}")
        return sqs, dynamodb
    except NoCredentialsError:
        log.critical("Credenciais da AWS não encontradas. Verifique seu ambiente.")
        sys.exit(1)
    except Exception as e:
        log.critical(f"Erro ao inicializar o Boto3: {e}")
        sys.exit(1)


sqs_client, dynamodb_client = create_aws_clients()


# --- SQS Worker ---


def _message_carrier(message):
    attributes = message.get("MessageAttributes", {})
    return {
        key: value["StringValue"]
        for key, value in attributes.items()
        if key in {"traceparent", "tracestate"} and value.get("StringValue")
    }


def process_message(message):
    """Processa uma única mensagem SQS e a insere no DynamoDB"""
    message_id = message.get("MessageId", "unknown")
    parent_context = propagate.extract(_message_carrier(message), context=Context())
    started_at = time.perf_counter()
    outcome = "success"

    with tracer.start_as_current_span(
        "sqs.process_evaluation",
        context=parent_context,
        kind=SpanKind.CONSUMER,
        attributes={
            "messaging.system": "aws_sqs",
            "messaging.operation.name": "process",
            "messaging.message.id": message_id,
        },
    ) as span:
        try:
            log.info(f"Processando mensagem ID: {message_id}")
            body = json.loads(message["Body"])
            span.set_attribute("feature_flag.name", body["flag_name"])

            # Gera um ID único para o item no DynamoDB
            event_id = str(uuid.uuid4())

            # Constrói o item no formato do DynamoDB
            item = {
                "event_id": {"S": event_id},
                "user_id": {"S": body["user_id"]},
                "flag_name": {"S": body["flag_name"]},
                "result": {"BOOL": body["result"]},
                "timestamp": {"S": body["timestamp"]},
            }

            dynamodb_client.put_item(TableName=DYNAMODB_TABLE_NAME, Item=item)
            log.info(f"Evento {event_id} (Flag: {body['flag_name']}) salvo no DynamoDB.")
            sqs_client.delete_message(
                QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"]
            )
            message_counter.add(1, {"messaging.outcome": outcome})

        except json.JSONDecodeError as error:
            outcome = "invalid_json"
            span.record_exception(error)
            span.set_status(Status(StatusCode.ERROR, outcome))
            message_counter.add(1, {"messaging.outcome": outcome})
            log.error(f"Erro ao decodificar JSON da mensagem ID: {message_id}")
        except ClientError as error:
            outcome = "aws_error"
            span.record_exception(error)
            span.set_status(Status(StatusCode.ERROR, outcome))
            message_counter.add(1, {"messaging.outcome": outcome})
            log.error(f"Erro do Boto3 ao processar {message_id}: {error}")
        except Exception as error:
            outcome = "unexpected_error"
            span.record_exception(error)
            span.set_status(Status(StatusCode.ERROR, outcome))
            message_counter.add(1, {"messaging.outcome": outcome})
            log.error(f"Erro inesperado ao processar {message_id}: {error}")
        finally:
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            processing_duration.record(elapsed_ms, {"messaging.outcome": outcome})


def sqs_worker_loop():
    """Loop principal do worker que ouve a fila SQS"""
    log.info("Iniciando o worker SQS...")
    while True:
        try:
            # Long-polling: espera até 20s por mensagens
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,  # Processa em lotes de até 10
                WaitTimeSeconds=20,
                MessageAttributeNames=["All"],
            )

            messages = response.get("Messages", [])
            if not messages:
                # Nenhuma mensagem, continua o loop
                continue

            log.info(f"Recebidas {len(messages)} mensagens.")

            for message in messages:
                process_message(message)

        except ClientError as e:
            log.error(f"Erro do Boto3 no loop principal do SQS: {e}")
            time.sleep(10)  # Pausa antes de tentar novamente
        except Exception as e:
            log.error(f"Erro inesperado no loop principal do SQS: {e}")
            time.sleep(10)


# --- Servidor Flask (Apenas para Health Check) ---


@app.route("/health")
def health():
    # Uma verificação de saúde real poderia checar a conexão com o DynamoDB/SQS
    return jsonify({"status": "ok"})


# --- Inicialização ---


def start_worker():
    """Inicia o worker SQS em uma thread separada"""
    worker_thread = threading.Thread(target=sqs_worker_loop, daemon=True)
    worker_thread.start()


# Inicia o worker SQS em uma thread de background
# Isso garante que ele inicie tanto com 'flask run' quanto com 'gunicorn'
start_worker()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8005))
    app.run(host="0.0.0.0", port=port, debug=False)
