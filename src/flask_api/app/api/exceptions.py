from typing import Any, Dict, Optional, Union

from flask import make_response
from flask import jsonify
from flask import current_app

class Response:
    """
    Construct a Flask response with the given status code and data.

    This class is used as a simple way to return a JSON response from
    an API endpoint. It is a factory function that returns a Flask
    response with the given status code and data.

    Args:
        status_code (int): The status code for the response
        data (Any): The data to include in the response

    Returns:
        Response: A Flask response with the given status code and data
    """
    def __new__(self, status_code: int, data: Any):
        """
        Construct a Flask response with the given status code and data.

        Args:
            status_code (int): The status code for the response
            data (Any): The data to include in the response

        Returns:
            Response: A Flask response with the given status code and data
        """
        return make_response(jsonify(data), status_code)

    
class BaseErrorResponse(Exception):
    """Base class for custom error responses
    
    This class is used as a base class for custom error responses. It
    provides a simple way to return a JSON response with a status code
    and some data.
    
    Attributes:
        status_code (int): The status code for the response
        default_err_msg (str): The default error message
        public (dict[str, str] | str | None): The public error message
        debug (dict[str, str] | str | None): The debug message
    """
    status_code: int
    default_err_msg: str
    public: Optional[Union[Dict[str, str], str]] = None
    debug: Optional[Union[Dict[str, str], str]] = None
    
    def __init__(self, public, debug) -> None:
        """Initialize the class
        
        Args:
            public (Optional[Union[Dict[str, str], str]]): The public error message
            debug (Optional[Union[Dict[str, str], str]]): The debug message
        """
        self.public = public
        self.debug = debug
        self._log()


    @property
    def errors(self) -> dict:
        """Return the errors

        Returns:
            dict: The errors
        """
        err_msg = self.public or self.default_err_msg
        return {"error": err_msg}

    
    def _log(self):
        """Log the debug message
        """
        if self.debug is None:
            return
        
        if isinstance(self.debug, list):
            for msg in self.debug:
                current_app.logger.debug(f"{type(self).__name__} = {msg}")
        else:
            current_app.logger.debug(f"{type(self).__name__} = {self.debug}")
            

class BadRequest(BaseErrorResponse):
    status_code = 400
    default_message = "Bad Request"

    def __init__(
            self,
            message: Optional[str] = None,
            debug: Optional[Union[Dict[str, str], str]] = None
    ) -> None:
        """Initialize the class
        
        Args:
            message (Optional[str]): The public error message
            debug (Optional[Union[Dict[str], str]]): The debug message
        """
        super().__init__(message or self.default_message, debug)


class Unauthorized(BaseErrorResponse):
    status_code = 401
    default_err_msg = "Unauthorized Client"
    
    def __init__(
            self, 
            public: Optional[Union[Dict[str, str], str]] = None,
            debug: Optional[Union[Dict[str, str], str]] = None
        ) -> None:
        super().__init__(self.public, self.debug)
        
        
class Forbidden(BaseErrorResponse):
    status_code = 403
    default_err_msg = "Forbidden"
    
    def __init__(
            self, 
            public: Optional[Union[Dict[str, str], str]] = None,
            debug: Optional[Union[Dict[str, str], str]] = None
        ) -> None:
        super().__init__(self.public, self.debug)


class ServerError(BaseErrorResponse):
    status_code = 500
    default_err_msg = "Internal Server Error"
    
    def __init__(
            self, 
            public: Optional[Union[Dict[str, str], str]] = None,
            debug: Optional[Union[Dict[str, str], str]] = None
        ) -> None:
        super().__init__(self.public, self.debug)