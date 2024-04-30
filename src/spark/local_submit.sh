#!/bin/bash

spark-submit --master spark://spark-master:7077 --deploy-mode client \
     --conf spark.pyspark.python="$(WORKDIR)"/"$(PEX_FOLDER)"/jobs.pex \
     "$(WORKDIR)"/jobs/entrypoint.py
