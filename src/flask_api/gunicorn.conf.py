# This gunicorn configuration file specifies the behavior of the
# Gunicorn WSGI HTTP Server for the Flask application.
#
# The following environment variables can be used to override the default
# settings:
#
# - WEB_ENDPOINT: the IP address and port to bind the server to (default:
#   "0.0.0.0:5000")
# - WEB_CONCURRENCY: the number of worker processes to use (default: the
#   number of CPU cores on the system multiplied by 2)
# - PYTHON_MAX_THREADS: the number of threads to use per worker process
#   (default: 1)
# - WEB_RELOAD: whether to reload the Flask application on code changes
#   (default: "false")
#
# These variables can be set as Docker environment variables or Kubernetes
# container environment variables.
#
# See the Gunicorn documentation for more information:
# <https://docs.gunicorn.org/en/stable/settings.html>

from distutils.util import strtobool
import multiprocessing
import os


bind = os.getenv("WEB_ENDPOINT", "0.0.0.0:5000")
"""The address to bind the server to.

This is the address that the server will listen on. The default is
"0.0.0.0:5000", which listens on all available network interfaces and port
5000.

This can be set to a string with the format "<ip>:<port>" or "<port>".
"""

accesslog = "-"
"""The file to write access logs to.

This can be "-" to log to stdout. The default is "-", which means that access
logs are not written to any file.
"""

access_log_format = "%(h)s %(l)s %(u)s %(t)s '%(r)s' %(s)s %(b)s '%(f)s' '%(a)s' in %(M)sms"
"""The format of the access logs.

This is a format string that is passed to the standard library's str.format()
function. The available format directives are documented in the Gunicorn
documentation:

<https://docs.gunicorn.org/en/stable/settings.html#access-log-format>
"""

workers = int(os.getenv("WEB_CONCURRENCY", multiprocessing.cpu_count() * 2))
"""The number of worker processes to use.

This is the number of processes that will be used to handle incoming HTTP
requests. The default is the number of CPU cores on the system multiplied by 2.
"""

threads = int(os.getenv("PYTHON_MAX_THREADS", 1))
"""The number of threads to use per worker process.

This is the number of threads that will be used in each worker process to
handle incoming HTTP requests. The default is 1.
"""

reload = bool(strtobool(os.getenv("WEB_RELOAD", "false")))
"""Whether to reload the Flask application on code changes.

This is whether to use the watchdog module to monitor the filesystem for
changes and automatically reload the Flask application when changes are
detected. The default is "false".
"""

timeout = 120
"""The timeout for worker processes.

This is the number of seconds that a worker process will be allowed to run
before it is terminated and restarted. The default is 120 seconds.
"""

preload = True
"""Whether to preload the Flask application.

This is whether to load the Flask application before starting the Gunicorn
server. The default is "true".
"""
