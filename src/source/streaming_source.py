import csv
import json

import httpx

SOURCE_FILE = "src/source/data/sample.csv"
API_URL = "http://localhost:5000/transactions"


def log_request(request):
    print(
        f"Request event hook: {request.method} {request.url} \
            - Waiting for response"
    )


def log_response(response):
    request = response.request
    print(
        f"Response event hook: {request.method} {request.url} \
            - Status {response.status_code}"
    )


def stream_transactions(file: str):
    with open(file, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("", None) is not None:
                row.pop("")
            yield json.dumps(row, indent=4)


def main():
    with httpx.Client(
        # event_hooks={"request": [log_request], "response": [log_response]}
    ) as client:
        for transaction in stream_transactions(SOURCE_FILE):
            # print(transaction)
            resp = client.post(API_URL, json=transaction)
            resp.raise_for_status()
            # print(resp.json())


if __name__ == "__main__":
    main()

# https://hynek.me/articles/what-to-mock-in-5-mins/
# https://www.b-list.org/weblog/2023/dec/08/mock-python-httpx/
