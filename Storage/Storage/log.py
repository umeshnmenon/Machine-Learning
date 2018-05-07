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
storage_logger = logging.getLogger("storage.log")
#log_level = getattr(storage_logger, log_level)
storage_logger.setLevel(log_level)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:process %(process)d:process name %(processName)s:%(funcName)s():line %(lineno)d:%(message)s')
if out_type == "file":
    stdout_handler = logging.FileHandler(log_file, mode='a')
else:
    stdout_handler = logging.StreamHandler(sys.stdout)

stdout_handler.setLevel(log_level)
stdout_handler.setFormatter(formatter)
storage_logger.addHandler(stdout_handler)
