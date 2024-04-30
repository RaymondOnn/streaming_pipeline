#!/bin/bash

#SPARK_WORKLOAD=$1
#
#echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"
#
#if [ "$SPARK_WORKLOAD" == "master" ];
#then
#    start-master.sh -p 7077
#elif [ "$SPARK_WORKLOAD" == "worker" ];
#then
#    start-worker.sh spark://spark-master:7077
#elif [ "$SPARK_WORKLOAD" == "history" ]
#then
#    start-history-server.sh
#fi

# Set environment variables
export SPARK_MODE="${SPARK_MODE:-master}"
export SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

# validate env variables SPARK_MODE, SPARK_MASTER_URL
error_code=0

# Auxiliary functions
print_validation_error() {
     error "$1"
     error_code=1
}

# Validate spark mode
case "$SPARK_MODE" in
master | worker) ;;

*)
     print_validation_error "Invalid mode $SPARK_MODE. Supported types are 'master/worker'"
     ;;
esac

# Validate worker node inputs
if [[ "$SPARK_MODE" == "worker" ]]; then
     if [[ -z "$SPARK_MASTER_URL" ]]; then
            print_validation_error "For worker nodes you need to specify the SPARK_MASTER_URL"
     fi
fi

[[ "$error_code" -eq 0 ]] || exit "$error_code"

# Init based on SPARK_MODE
if [ "$SPARK_MODE" == "master" ]; then
    # Master constants
     EXEC=$(command -v start-master.sh)
     ARGS=()
     echo "** Starting Spark in master mode **"
else
    # Worker constants
     EXEC=$(command -v start-worker.sh)
     ARGS=("$SPARK_MASTER_URL")
     echo "** Starting Spark in worker mode **"
fi

exec "$EXEC" "${ARGS[@]-}"