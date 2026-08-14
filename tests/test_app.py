from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError, NoCredentialsError


def test_health_check(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}


def test_process_message_stores_event_and_deletes_message(
    app_module, aws_clients, message, monkeypatch
):
    sqs_client, dynamodb_client = aws_clients
    monkeypatch.setattr(app_module.uuid, "uuid4", Mock(return_value="event-1"))

    app_module.process_message(message)

    dynamodb_client.put_item.assert_called_once_with(
        TableName="ToggleMasterAnalytics",
        Item={
            "event_id": {"S": "event-1"},
            "user_id": {"S": "user-1"},
            "flag_name": {"S": "checkout"},
            "result": {"BOOL": True},
            "timestamp": {"S": "2026-08-13T12:00:00Z"},
        },
    )
    sqs_client.delete_message.assert_called_once_with(
        QueueUrl="https://sqs.example.test/analytics", ReceiptHandle="receipt-1"
    )


def test_process_message_keeps_invalid_json_on_queue(app_module, aws_clients, message):
    sqs_client, dynamodb_client = aws_clients
    message["Body"] = "not-json"

    app_module.process_message(message)

    dynamodb_client.put_item.assert_not_called()
    sqs_client.delete_message.assert_not_called()


def test_process_message_keeps_message_when_aws_fails(app_module, aws_clients, message):
    sqs_client, dynamodb_client = aws_clients
    dynamodb_client.put_item.side_effect = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "busy"}},
        "PutItem",
    )

    app_module.process_message(message)

    sqs_client.delete_message.assert_not_called()


def test_process_message_handles_unexpected_payload(app_module, aws_clients, message):
    sqs_client, dynamodb_client = aws_clients
    message["Body"] = '{"user_id":"user-1"}'

    app_module.process_message(message)

    dynamodb_client.put_item.assert_not_called()
    sqs_client.delete_message.assert_not_called()


def test_worker_processes_received_messages(app_module, aws_clients, message, monkeypatch):
    sqs_client, _ = aws_clients
    sqs_client.receive_message.side_effect = [
        {"Messages": [message]},
        KeyboardInterrupt,
    ]
    process_message = Mock()
    monkeypatch.setattr(app_module, "process_message", process_message)

    with pytest.raises(KeyboardInterrupt):
        app_module.sqs_worker_loop()

    process_message.assert_called_once_with(message)
    sqs_client.receive_message.assert_called_with(
        QueueUrl="https://sqs.example.test/analytics",
        MaxNumberOfMessages=10,
        WaitTimeSeconds=20,
    )


def test_worker_continues_when_queue_is_empty(app_module, aws_clients):
    sqs_client, _ = aws_clients
    sqs_client.receive_message.side_effect = [{}, KeyboardInterrupt]

    with pytest.raises(KeyboardInterrupt):
        app_module.sqs_worker_loop()

    assert sqs_client.receive_message.call_count == 2


def test_worker_retries_after_aws_error(app_module, aws_clients, monkeypatch):
    sqs_client, _ = aws_clients
    sqs_client.receive_message.side_effect = [
        ClientError({"Error": {"Code": "ServiceUnavailable", "Message": "down"}}, "ReceiveMessage"),
        KeyboardInterrupt,
    ]
    sleep = Mock()
    monkeypatch.setattr(app_module.time, "sleep", sleep)

    with pytest.raises(KeyboardInterrupt):
        app_module.sqs_worker_loop()

    sleep.assert_called_once_with(10)


def test_worker_retries_after_unexpected_error(app_module, aws_clients, monkeypatch):
    sqs_client, _ = aws_clients
    sqs_client.receive_message.side_effect = [RuntimeError("network failure"), KeyboardInterrupt]
    sleep = Mock()
    monkeypatch.setattr(app_module.time, "sleep", sleep)

    with pytest.raises(KeyboardInterrupt):
        app_module.sqs_worker_loop()

    sleep.assert_called_once_with(10)


def test_start_worker_creates_daemon_thread(app_module, monkeypatch):
    thread = Mock()
    thread_factory = Mock(return_value=thread)
    monkeypatch.setattr(app_module.threading, "Thread", thread_factory)

    app_module.start_worker()

    thread_factory.assert_called_once_with(target=app_module.sqs_worker_loop, daemon=True)
    thread.start.assert_called_once_with()


def test_startup_requires_aws_configuration(app_module, monkeypatch):
    monkeypatch.setattr(app_module, "AWS_REGION", None)

    with pytest.raises(SystemExit) as error:
        app_module.validate_config()

    assert error.value.code == 1


def test_startup_handles_missing_aws_credentials(app_module, monkeypatch):
    monkeypatch.setattr(app_module.boto3, "Session", Mock(side_effect=NoCredentialsError()))

    with pytest.raises(SystemExit) as error:
        app_module.create_aws_clients()

    assert error.value.code == 1


def test_startup_handles_unexpected_boto_error(app_module, monkeypatch):
    monkeypatch.setattr(
        app_module.boto3, "Session", Mock(side_effect=RuntimeError("invalid AWS setup"))
    )

    with pytest.raises(SystemExit) as error:
        app_module.create_aws_clients()

    assert error.value.code == 1
