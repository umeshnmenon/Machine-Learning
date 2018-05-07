import logging
from autologging import logged
from httpserver.response_codes import SimpleHTTPStatusOK, SimpleHTTPErrorInternalError
@logged(logging.getLogger("http.server.log"))
class GenericRequestHandler:
    def process_request(self, func=None, args=None):
        body = []
        try:
            if func is None:
                body = self.index()
            else:
                body = getattr(self, func)(args)
            header = 200
        except Exception as e:
            self.__log.error("Error while processing request. Error: {}".format(str(e)))
            body["error"] = str(e)
            header = 500
        return header, body

    def index(self):
        return {}