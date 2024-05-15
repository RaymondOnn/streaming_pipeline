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
#
# SPARK_MODE:
#   Optional. Possible values are 'master' or 'worker'.
#   Default value is 'master'.
# SPARK_MASTER_URL:
#   Optional. Address of the Spark master.
#   It is required for worker nodes.
export SPARK_MODE="${SPARK_MODE:-master}"
export SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

# Validate env variables SPARK_MODE, SPARK_MASTER_URL
error_code=0

# Auxiliary functions
print_validation_error() {
    # Prints the error message and exits with non-zero status code
    #
    # Arguments:
    #   $1 - error message
    error "$1"
    error_code=1
}

# Validate spark mode
case "$SPARK_MODE" in
master | worker) ;;

*)
    # Invalid mode
    #
    # Arguments:
    #   $1 - invalid mode
    print_validation_error "Invalid mode $SPARK_MODE. Supported types are 'master/worker'"
    ;;
esac

# Validate worker node inputs
if [[ "$SPARK_MODE" == "worker" ]]; then
    # Master URL is required for worker nodes
    #
    # Arguments:
    #   $1 - environment variable name
    #   $2 - environment variable value
    if [[ -z "$SPARK_MASTER_URL" ]]; then
        print_validation_error "For worker nodes you need to specify the SPARK_MASTER_URL"
    fi
fi

# Exit if there are validation errors
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

# Execute the corresponding Spark script with the arguments
exec "$EXEC" "${ARGS[@]-}"
