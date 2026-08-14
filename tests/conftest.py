import importlib
import sys
from unittest.mock import Mock

import pytest


@pytest.fixture(scope="session")
def monkeypatch_session():
    with pytest.MonkeyPatch.context() as monkeypatch:
        yield monkeypatch


@pytest.fixture(scope="session")
def app_module(monkeypatch_session):
    monkeypatch_session.setenv("AWS_REGION", "us-east-1")
    monkeypatch_session.setenv("AWS_SQS_URL", "https://sqs.example.test/analytics")
    monkeypatch_session.setenv("AWS_DYNAMODB_TABLE", "ToggleMasterAnalytics")

    import boto3
    import threading

    session = Mock()
    session.client.side_effect = [Mock(), Mock()]
    monkeypatch_session.setattr(boto3, "Session", Mock(return_value=session))
    monkeypatch_session.setattr(threading, "Thread", Mock(return_value=Mock()))
    sys.modules.pop("app", None)
    return importlib.import_module("app")


@pytest.fixture
def aws_clients(app_module, monkeypatch):
    sqs_client = Mock()
    dynamodb_client = Mock()
    monkeypatch.setattr(app_module, "sqs_client", sqs_client)
    monkeypatch.setattr(app_module, "dynamodb_client", dynamodb_client)
    return sqs_client, dynamodb_client


@pytest.fixture
def client(app_module, aws_clients):
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


@pytest.fixture
def message():
    return {
        "MessageId": "message-1",
        "ReceiptHandle": "receipt-1",
        "Body": (
            '{"user_id":"user-1","flag_name":"checkout",'
            '"result":true,"timestamp":"2026-08-13T12:00:00Z"}'
        ),
    }
