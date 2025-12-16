import pytest

from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

#
def test_predict_success(client):
    response = client.post("/query", json={"query": "O que é a Constituição?"})

    assert response.status_code == 200
    data = response.json()

    assert "response" in data

#
@pytest.mark.parametrize("query", [[], None, 35.0, {"test": "hello"}, (1, 2)])
def test_predict_invalid_query(client, query):
    payload = {"query": query}

    response = client.post("/query", json=payload)

    assert response.status_code == 422

#
@pytest.mark.parametrize("body", [{"queries": "Test Query"}, {"not_a_query": "invalid key!"}, {"user_input": "wrong key!"} ,{"": ""}, {}, "", None, [], 123, 3.1416])
def test_predict_invalid_body(client, body):
    payload = body

    response = client.post("/query", json=payload)

    assert response.status_code == 422
