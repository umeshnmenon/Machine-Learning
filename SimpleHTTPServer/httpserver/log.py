import logging, sys, os #, time, logging.config
import ConfigParser

#read config files
config_file = "settings.ini"
root_folder = os.path.dirname(os.path.realpath(__file__))
config_file = root_folder + '/' + config_file
config = ConfigParser.ConfigParser()
config.read(config_file)

log_level_key = 'log_level'
consumer_log_level = config.get('log', log_level_key).upper()

# or we can just use getattr() instead as commented below
if consumer_log_level == 'CRITICAL':
    log_level = logging.CRITICAL
elif consumer_log_level == 'ERROR':
    log_level = logging.ERROR
elif consumer_log_level == 'WARNING':
    log_level = logging.WARNING
elif consumer_log_level == 'INFO':
    log_level = logging.INFO
else:
    log_level = logging.DEBUG

out_type = config.get('log', 'out_type')
log_location = config.get('log', 'location')
log_file = log_location + "storage.log"

# Handles all the loggers to use across the project
httpserver_logger = logging.getLogger("http.server.log")
#log_level = getattr(httpserver_logger, log_level)
httpserver_logger.setLevel(log_level)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:process %(process)d:process name %(processName)s:%(funcName)s():line %(lineno)d:%(message)s')


if out_type == "file":
    stdout_handler = logging.FileHandler(log_file, mode='a')
else:
    stdout_handler = logging.StreamHandler(sys.stdout)

stdout_handler.setLevel(log_level)
stdout_handler.setFormatter(formatter)
httpserver_logger.addHandler(stdout_handler)

# HTTP Access logger
access_log_file = log_location + "http.server.access.log"
access_logger = logging.getLogger("http.server.access.log")
access_logger.setLevel(log_level)
if out_type == "file":
    stdout_handler_access_log = logging.FileHandler(access_log_file, mode='a')
else:
    stdout_handler_access_log = logging.StreamHandler(sys.stdout)

stdout_handler_access_log.setLevel(log_level)
stdout_handler_access_log.setFormatter(formatter)
access_logger.addHandler(stdout_handler_access_log)

# HTTP Error logger
error_log_file = log_location + "http.server.error.log"
error_logger = logging.getLogger("http.server.error.log")
error_logger.setLevel(log_level)
if out_type == "file":
    stdout_handler_error_log = logging.FileHandler(error_log_file, mode='a')
else:
    stdout_handler_error_log = logging.StreamHandler(sys.stdout)

stdout_handler_error_log.setLevel(log_level)
stdout_handler_error_log.setFormatter(formatter)
error_logger.addHandler(stdout_handler_error_log)