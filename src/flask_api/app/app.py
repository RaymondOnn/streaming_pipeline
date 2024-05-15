import traceback
from flask import Flask, current_app
from flask_smorest import Api

from .api.transactions import blp as TransactionBlueprint
from .api.transactions import minio_client, producer, TransactionData
from .api.exceptions import BaseErrorResponse, Response, ServerError, BadRequest
from .fx import object_store, pubsub,registry
from .utils.settings import APIConfig
from .utils.settings import FXConfig
from .utils.settings import KafkaConfig


# def setup():
# Set up storage, schema registry, and Kafka topic for streaming.
sr_client = registry.get_schema_registry_client()
sr_client.register_schema(topic=KafkaConfig().topic, class_object=TransactionData)

for bucket in FXConfig().buckets:
    object_store.get_bucket(minio_client, bucket)

admin_client = pubsub.get_admin_client()
pubsub.create_topic(admin_client, KafkaConfig().topic)

def check_services():
    pubsub.has_valid_value_serializer(producer)


def handle_error(exception: Exception):
    """
    Handles an exception and returns an appropriate response based on the type of exception.

    Parameters:
        exception (Exception): The exception that was raised.

    Returns:
        Response: The response object containing the appropriate status code and error message.

    """
    if isinstance(exception, BaseErrorResponse):
        return Response(exception.status_code, exception.errors)

    log_msg = f"{type(exception).__name__} - {exception}"
    current_app.logger.error(log_msg)

    if current_app.debug:
        current_app.logger.error(
            log_msg + "\n" + traceback.format_exc()
        )

    return Response(
        ServerError.status_code,
        data={"error": ServerError.default_err_msg},
    )


def create_app() -> Flask:
    """
    Creates a Flask application instance.

    This function initializes a Flask application with the 
    specified configuration. It registers the `TransactionBlueprint` blueprint 
    and sets up the necessary routes. It also sets up a catch-all route to 
    handle invalid paths and an error handler for unhandled exceptions.

    Returns:
        Flask: The Flask application instance.

    """
    app = Flask(__name__)
    app.config.from_object(APIConfig())
    api = Api(app)
    api.register_blueprint(TransactionBlueprint)

    @app.route("/")
    def index():
        return Response(200, data="Server is up")

    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def _catch_all(*args, **kwargs):
        try:
            raise BadRequest("Invalid path", debug=kwargs)
        except Exception as e:
            current_app.logger.error(
                "Catch all error - Unhandled exception " 
                + f"{type(e).__name__}: {e}",
                exc_info=True)
            return Response(
                ServerError.status_code,
                data={"error": ServerError.default_err_msg})

    app.errorhandler(Exception)(handle_error)

    # setup()
    # app.app_context().push()  # i.e. before_first_request
    return app


if __name__ == "__main__":
    create_app().run(port=5000)
