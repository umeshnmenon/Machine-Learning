import logging
from autologging import logged
from httpserver.response_codes import SimpleHTTPStatusOK, SimpleHTTPErrorInternalError
from generic_request_handler import GenericRequestHandler
@logged(logging.getLogger("http.server.log"))
class HealthcheckRequestHandler(GenericRequestHandler):
    # def __init__(self, config=None):
    #     self.config = config

    def index(self):
        """
        Serve the HTTP Request
        :return:
        """
        return self.check_health()

    def check_health(self):
        # returns nothing but a 200 status
        return 200, SimpleHTTPStatusOK
