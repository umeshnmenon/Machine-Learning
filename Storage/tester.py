import ConfigParser
import os, time
from log import *
from storage import *
from cacheserver import CacheServer
# load the config
config_file = "settings.ini"
root_folder = os.path.dirname(os.path.realpath(__file__))
config_file = root_folder + '/' + config_file
config = ConfigParser.ConfigParser()
config.read(config_file)

# start the Internal Cache Server
cache_server = CacheServer(config)
cache_server.start()
time.sleep(10)
cache_server.join()

# connect to the storage
storage = Storage('internal', 'localhost', '8010')

# set some values
storage.set('id', 1001)

# get the value
print(storage.get('id'))

# set value with expiry with 10 seconds
storage.setex('cookie', 'abcedefgeh', 10)
print(storage.get('cookie')) # this will return abcedefgeh
#wait for 10 seconds
time.sleep(11)
print(storage.get('cookie')) # this will return None

# set fixed size dictionary of size 5
storage.push('top_5_articles', 'article 1',  5)
storage.push('top_5_articles', 'article 2',  5)
storage.push('top_5_articles', 'article 3',  5)
storage.push('top_5_articles', 'article 4',  5)
storage.push('top_5_articles', 'article 5',  5)
storage.push('top_5_articles', 'article 6',  5) # at this step article 1 will be popped and article 6 will be pushed to keep the total count 5

print(storage.get('top_5_articles'))

# set fixed size dictionary of size 5 and expiry 10 seconds
storage.push('top_5_articles', 'article 1',  5)
storage.push('top_5_articles', 'article 2',  5)
storage.push('top_5_articles', 'article 3',  5)
storage.push('top_5_articles', 'article 4',  5)
storage.push('top_5_articles', 'article 5',  5)
storage.push('top_5_articles', 'article 6',  5) # at this step article 1 will be popped and article 6 will be pushed to keep the total count 5

print(storage.get('top_5_articles')) # this will return the list of articles
time.sleep(11)
print(storage.get('top_5_articles')) # this will return None
