spark-submit --master yarn --deploy-mode cluster \
--py-files sdbl_lib.zip \
--files conf/sdbl.conf,conf/spark.conf,log4j.properties \
--driver-cores 2 \
--driver-memory 3G \
--conf spark.driver.memoryOverhead=1G
sbdl_main.py qa 2022-08-03

spark-submit --master spark://spark-master:7077 --deploy-mode client \
--conf spark.pyspark.python=$(WORKDIR)/$(PEX_FOLDER)/jobs.pex \
$(WORKDIR)/jobs/entrypoint.py
