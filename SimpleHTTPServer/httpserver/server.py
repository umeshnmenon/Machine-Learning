import BaseHTTPServer #, SimpleHTTPServer
import ssl
from threading import Thread, Event
from autologging import logged
import logging
from httpserver.request_handler import RequestHandler
from helpers.utils import Utils
import time
@logged(logging.getLogger("http.server.log"))
class HTTPServer(Thread):
    """
    A simple, minimal HTTP server
    """
    CONFIG_KEY = 'http'
    THREAD_NAME = 'simple-http-server'
    def __init__(self, config):
        self.__log.info("Instantiating Simple HTTP Server")
        Thread.__init__(self)
        self.setName(self.THREAD_NAME)
        self._stop = Event()
        self.setDaemon(True)
        self.config = config

    def stop(self):
        self.stop_httpd()
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def _request_handler(self, *args):
        RequestHandler(self.config, *args)

    def run(self):
        self.__log.info("Staring Simple HTTP Server")
        host_key = 'http_host'
        port_key = 'http_port'
        ssl_port_key = 'http_ssl_port'
        cert_file_key = 'http_cert_file'
        host = self.config.get(self.CONFIG_KEY, host_key) #Utils.get_env_var(host_key, self.config.get(self.CONFIG_KEY, host_key), self.__log)
        ssl_enabled = bool(self.config.get(self.CONFIG_KEY, 'http_ssl_enabled'))
        #port = Utils.get_env_var(port_key, self.config.get(self.CONFIG_KEY, port_key), self.__log) if ssl_enabled else Utils.get_env_var(ssl_port_key, self.config.get(self.CONFIG_KEY, ssl_port_key), self.__log)
        port = self.config.get(self.CONFIG_KEY, port_key) if ssl_enabled else self.config.get(self.CONFIG_KEY, ssl_port_key)
        cert_file = self.config.get(self.CONFIG_KEY, cert_file_key) #Utils.get_env_var(cert_file_key, self.config.get(self.CONFIG_KEY, cert_file_key), self.__log)
        self.__log.debug("host: {}".format(host))
        self.__log.debug("port: {}".format(port))
        self.__log.debug("cert file: {}".format(cert_file))
        self.httpd = BaseHTTPServer.HTTPServer((host, int(port)), self._request_handler) #SimpleHTTPServer.SimpleHTTPRequestHandler
        if ssl_enabled == 'True':
            self.__log.info("Wrapping SSL")
            self.httpd.socket = ssl.wrap_socket(self.httpd.socket, server_side=True,
                                       certfile=cert_file)
        self.httpd.serve_forever()
        #self.http_thread = Thread(target=self.httpd.serve_forever(), )
        #self.http_thread.daemon = True

        # Starting the HTTP server
        #try:
            #self.http_thread.start()
        #except Exception as e:
        #    self.__log.error("Error while starting HTTP Server")
        #    self.__log.error(str(e))
        #    self.httpd.shutdown()
        # Wait until HTTP server is ready
        time.sleep(1)

    def stop_httpd(self):
        self.__log.info("Shtting down HTTP Server...")
        self.httpd.shutdown()
        #self.http_thread.stop()
        self.__log.info("HTTP Server Shutdown completed")