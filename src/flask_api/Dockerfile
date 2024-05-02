FROM python:3.11-slim-buster

ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_HOME=/flask_api

WORKDIR ${FLASK_HOME}
# ENV VIRTUAL_ENV=venv
# ENV VENV_DIR=${FLASK_HOME}/${VIRTUAL_ENV}
COPY . .

# RUN python -m venv ${VIRTUAL_ENV} \
#     && ${VIRTUAL_ENV}/bin/pip3 install --no-cache-dir --requirement requirements.api.txt
RUN pip3 install --no-cache-dir --requirement requirements.api.txt
# ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
EXPOSE 5000
# CMD ["python", "-m", "flask", "run"]

CMD [ \
    "python", "-m", "gunicorn", \
    "-b", "0.0.0.0:5000", \
    "--workers", "1", \
    "--timeout", "120", \
    "--preload", \
    "app.app:create_app()" \
]