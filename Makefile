SHELL := /bin/bash # Tell Make this file is written with Bash as shell
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c # Bash strict mode
.DELETE_ON_ERROR:   # if a Make rule fails, it’s target file is deleted
.DEFAULT_GOAL := help
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules


WORKDIR = $(shell pwd)
# Get package name from pwd
PACKAGE_NAME := $(shell basename $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))
VENV = .venv
VENV_DIR=$(WORKDIR)/${VENV}
VENV_BIN=$(VENV_DIR)/bin
PYTHON=${VENV_BIN}/python3
PIP=$(VENV)/bin/pip

GITIGNORE_PKGS=venv,python,JupyterNotebooks,Pycharm,VisualStudioCode,macOS
REPO_NAME=

FLASK_DIR = src/flask_api
SPARK_CONTAINER  = spark-node
SPARK_DOCKER_DIR = src/spark/docker
SPARK_WORKDIR = /opt/spark/workdir
AIRFLOW_SCHEDULER=airflow-webserver


#################################### Functions ###########################################
# Function to check if package is installed else install it.
define install_pip_pkg_if_not_exist
	@for pkg in ${1} ${2} ${3}; do \
		echo ${1} ${2} ${3}
		# if ! command -v "$${pkg}" >/dev/null 2>&1; then \
		# 	echo "installing $${pkg}"; \
		# 	$(PYTHON) -m pip install $${pkg}; \
		# fi;\
	done
endef

# Function to create python virtualenv if it doesn't exist
define create-venv
	$(call install_pip_pkg_if_not_exist,virtualenv)

	@if [ ! -d ".$(PACKAGE_NAME)_venv" ]; then \
		$(PYTHON) -m virtualenv ".$(PACKAGE_NAME)_venv" -p $(PYTHON) -q; \
		.$(PACKAGE_NAME)_venv/bin/python -m pip install -qU pip; \
		echo "\".$(PACKAGE_NAME)_venv\": Created successfully!"; \
	fi;
	@echo "Source virtual environment before tinkering"
	@echo -e "\tRun: \`source .$(PACKAGE_NAME)_venv/bin/activate\`"
endef

########################################### END ##########################################

.PHONY: help
help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

check:
	. $(VENV_BIN)/activate


boilerplate:  ## Add simple 'README.md' and .gitignore
	@echo "# $(PACKAGE_NAME)" | sed 's/_/ /g' >> README.md
	@curl -sL https://www.gitignore.io/api/$(GITIGNORE_PKGS)> .gitignore

# ------------------------------------ Version Control -----------------------------------

git_init:  ## Create a new git repository and add boilerplate code.
	@git init -q
	@$(MAKE) -C $(CURDIR) boilerplate
	@git add .gitignore README.md
	git commit -nm'Add README and .gitignore files <automated msg>'

gh_init:
	gh repo create --public testRepo

venv_create: requirements.txt ## create virtual environment
	python3 -m venv $(VENV) \
		&& chmod +x $(VENV)/bin/activate \
		&& make venv_install

venv_install: requirements.txt
	$(PIP) install -r requirements.txt

venv: venv_create  ## activate virtual environment
	. $(VENV_BIN)/activate

.PHONY: clean
clean:
	find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf

.PHONY: clean-all
clean_all: ## cleanup files and remove virtual environment
	rm -rf __pycache__ *.pyc .pytest_cache;
	rm -rf $(VENV) || exit 0

.PHONY: spark_dev
spark_dev: ## start spark dev notebook
	docker compose -f ./$(SPARK_DOCKER_DIR)/docker-compose.dev.yaml up

.PHONY: spark_bash
spark_bash: ## stop airflow containers
	docker compose exec -it $(SPARK_CONTAINER) /bin/bash

.PHONY: spark_pex
spark_pex: $(shell find ./src/spark/app -type f) ## start airflow bash shell
	rm -rf ./src/spark/pex;
	docker compose exec $(SPARK_CONTAINER) /bin/bash -c \
		'pex --python=python3 --inherit-path=prefer \
			-r $(SPARK_WORKDIR)/requirements.spark.txt \
			-D $(SPARK_WORKDIR) \
			-o $(SPARK_WORKDIR)/pex/deps.pex'

.PHONY: spark_submit
spark_submit:  ## start airflow bash shell
	docker compose exec $(SPARK_CONTAINER) /bin/bash -c \
		'spark-submit \
			--conf spark.pyspark.python=$(SPARK_WORKDIR)/pex/deps.pex \
			$(SPARK_WORKDIR)/app/main.py'

.PHONY: flask_img
flask_img:  ## start airflow bash shell
	docker image build --no-cache  -f $(FLASK_DIR)/Dockerfile -t flask_api $(FLASK_DIR)

.PHONY: flask_dev
flask_dev:  ## start airflow bash shell
	flask --app $(FLASK_DIR)/app.py run --reload

.PHONY: flask_local_start
flask_local_start:  ## start airflow bash shell
	COMPOSE_PROFILES=minio,kafka docker compose up -d;
	make flask_dev

.PHONY: flask_local_stop
flask_local_stop:  ## start airflow bash shell
	COMPOSE_PROFILES=minio,kafka docker compose down
# .PHONY: airflow-img
# airflow_img: $(shell find src/airflow -type f) ## build docker image for airflow
# 	docker compose -f src/airflow/docker-compose.yaml build  --no-cache
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.spark:spark-avro_2.12:3.5.0 \
# --files app/conf/app.conf \

.PHONY: api_start
api_start:
	COMPOSE_PROFILES=api,minio,kafka docker compose -f src/docker/docker-compose.ingest.yaml up -d

.PHONY: api_stop
api_stop:
	COMPOSE_PROFILES=api,minio,kafka docker compose -f src/docker/docker-compose.ingest.yaml down

.PHONY: start_dev
start_dev:
	COMPOSE_PROFILES=api,minio,kafka,spark,postgres docker compose up -d

.PHONY: stop_dev
stop_dev:
	COMPOSE_PROFILES=api,minio,kafka,spark,postgres docker compose down

.PHONY: start
start:
	COMPOSE_PROFILES=api,minio,kafka,spark,db docker compose up -d

.PHONY: stop
stop:
	COMPOSE_PROFILES=api,minio,kafka,spark,db  docker compose down

.PHONY: restart
restart:
	make stop && make start

.PHONY: test
test:
	python run_tests.py
