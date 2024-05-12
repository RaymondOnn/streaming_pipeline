import csv
import json

import httpx
from loguru import logger

SOURCE_FILE = "src/source/data/sample.csv"
API_URL = "http://localhost:5000/transactions"


# Event hooks for logging requests and responses
# https://www.python-httpx.org/advanced/#event-hooks


def log_request(request):
    """Log requests before they are sent"""
    logger.debug(
        f"Request event hook: {request.method} {request.url}"
        + " - Waiting for response"
    )


def log_response(response):
    """Log responses after they are received"""
    request = response.request
    logger.debug(
        f"Response event hook: {request.method} {request.url}"
        + f" - Status {response.status_code}"
    )


def stream_transactions(file_path: str):
    """Stream transactions from a CSV file"""
    with open(file_path, newline="") as file:
        reader = csv.DictReader(file)
        for transaction in reader:
            yield json.dumps(transaction)


def main():
    """Send transactions to the server"""
    with httpx.Client(event_hooks={"response": [log_response]}) as client:
        for transaction in stream_transactions(SOURCE_FILE):
            client.post(API_URL, json=transaction)


if __name__ == "__main__":
    main()

# https://hynek.me/articles/what-to-mock-in-5-mins/
# https://www.b-list.org/weblog/2023/dec/08/mock-python-httpx/
