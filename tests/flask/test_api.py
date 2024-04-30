import pytest

from src.flask_api.app import create_app


@pytest.fixture()
def client():
    app = create_app()
    app.config["TESTING"] = True

    with app.app_context():
        with app.test_client() as client:
            yield client


# # def test_get(client):
# #     response = client.get("/")
# #     assert ___ in response.data
#
# def test_missing_config():
#     pass
#
#
# def test_transactions_post(client):
#     response = client.post("/transactions", json={"field": "payload"})
#     print(response.get_json())
#     assert 1 == 2
