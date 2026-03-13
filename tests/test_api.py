from fastapi.testclient import TestClient

from hormuz_index.api import create_app
from hormuz_index.config import Settings


def test_health_endpoint(tmp_path) -> None:
    settings = Settings.load()
    settings = settings.__class__(**{**settings.__dict__, "database_path": tmp_path / "api.db"})
    client = TestClient(create_app(settings))
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
