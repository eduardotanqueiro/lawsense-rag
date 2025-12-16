import pytest
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json().get("status") == "okay running"

def test_root():
    r = client.get("/")
    assert r.status_code == 200
    assert "LawSense RAG is running" in r.text

def test_query():
    r = client.post("/query", json={"query": "O que é a Constituição?"})
    assert r.status_code == 200
    assert "response" in r.json()