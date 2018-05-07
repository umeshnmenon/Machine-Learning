import ConfigParser
import os, time
from log import *
from server import *

# load the config
config_file = "settings.ini"
root_folder = os.path.dirname(os.path.realpath(__file__))
config_file = root_folder + '/' + config_file
config = ConfigParser.ConfigParser()
config.read(config_file)

# start the http server
http_server = HTTPServer(config)
http_server.start()
time.sleep(10)
http_server.join()

