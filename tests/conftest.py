import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from fastapi.testclient import TestClient
from api.main import app


@pytest.fixture
def test_app():
    return app


@pytest.fixture
def client(test_app):
    with TestClient(test_app) as client:
        yield client

@pytest.fixture
def sample_queries():
    return [
        "direitos fundamentais na Constituição",
        "contrato de crédito abusivo",
        "princípio da separação de poderes"
    ]