FROM python:3.11-slim-buster

ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0

WORKDIR /flask_api
COPY . .

RUN --mount=type=bind,source=requirements.api.txt,target=/tmp/requirements.txt \
    pip install --no-cache-dir --user --requirement /tmp/requirements.txt

EXPOSE 5000
CMD ["python", "-m", "flask", "run"]
