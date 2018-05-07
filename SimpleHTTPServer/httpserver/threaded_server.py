from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
import threading
from httpserver.request_handler import RequestHandler
from autologging import logged
import logging
import ssl
@logged(logging.getLogger("http.server.log"))
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

@logged(logging.getLogger("httpserver.log"))
class ThreadedHTTPServerLauncher:
    """Helps launch the ThreadedHTTPServer class"""
    def __init__(self, config):
        self.__log.info("Instantiating Simple Threaded HTTP Server")
        self.config = config

    def start(self):
        self.__log.info("Staring Simple Threaded HTTP Server")
        host_key = 'http_host'
        port_key = 'http_port'
        ssl_port_key = 'http_ssl_port'
        cert_file_key = 'http_cert_file'
        host =  self.config.get(self.CONFIG_KEY, host_key)
        ssl_enabled = bool(self.config.get(self.CONFIG_KEY, 'http_ssl_enabled'))
        port = self.config.get(self.CONFIG_KEY, port_key)
        cert_file = self.config.get(self.CONFIG_KEY, cert_file_key)
        self.__log.debug("host: {}".format(host))
        self.__log.debug("port: {}".format(port))
        self.__log.debug("cert file: {}".format(cert_file))
        self.httpd = ThreadedHTTPServer((host, int(port)),
                                               RequestHandler(self.config))
        if ssl_enabled == 'True':
            self.__log.info("Wrapping SSL")
            self.httpd.socket = ssl.wrap_socket(self.httpd.socket, server_side=True,
                                                certfile=cert_file)
        self.httpd.serve_forever()

    def stop_httpd(self):
        self.__log.info("Shtting down HTTP Server...")
        self.httpd.shutdown()
        self.__log.info("HTTP Server Shutdown completed")