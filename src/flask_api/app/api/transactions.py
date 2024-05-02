try:
    from flask import request
    from flask.views import MethodView
    from flask_smorest import Blueprint
    from ..fx.sort_requests import sort_requests
except Exception as e:
    print(f"Error: {e}")


blp = Blueprint("transactions", __name__, description="abc")


# Note: model_json_schema vs model_dump_json
@blp.route("/transactions")
class Transaction(MethodView):
    def post(self):
        # TODO: Test if wrong data triggers validation error
        # TODO: Test if data reaches API via POST request
        # TODO: Test if data validation is as expected.
        # - https://github.com/hsiangyuwen/flask-request-validator/blob/main/req_validator/api.py # noqa
        resp = request.get_json()
        sort_requests(resp)
        return resp
