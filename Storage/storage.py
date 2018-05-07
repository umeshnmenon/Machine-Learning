from redis_storage import RedisStorage
from cache_storage import CacheStorage
from fixed_size_dict import FixedSizeDict
from autologging import logged
import time
import logging
@logged(logging.getLogger("storage.log"))
class Storage:
    """
    Creates Cache storage based on the type property passed. Currently supports Redis and a simple dictionary based internal cache.
    Default is dictionary based internal cache
    """
    def __init__(self, type, host, port, db):
        self.__log.info("Instantiating Storage...")

        self._cache = None
        if type == 'redis':
            self.__log.info("Connecting to Redis Cache")
            self._cache = RedisStorage(host, port, db)
        else:
            self.__log.info("Connecting to Simple Internal Cache")
            #self._cache = Cache(config) # Moving away from singleton way and getting it from Cache Server
            self._cache = CacheStorage(host, port)

    @property
    def cache(self):
        return self._cache

    def __call__(self, *args, **kwargs):
        return self._cache

    def set(self, key, val):
        self.__log.info("Setting the value in Cache")
        self.__log.debug("key: {}, value: {}".format(key, val))
        self._cache.set(key, val)

    def get(self, key):
        self.__log.info("Getting the value from Cache")
        self.__log.debug("key: {}".format(key))
        return self._cache.get(key)

    def setex(self, key, val, expiry):
        self.__log.info("Setting the value in Cache with ttl")
        self.__log.debug("key: {}, value: {}, expiry: {}".format(key, val, expiry))
        self._cache.setex(name=key, time=expiry, value=val)

    def push(self, key, value, size, dict_key=None):
        """
        Pushes the value to a fixed size dictionary in storage
        :return:
        """
        dict_val = self._append_to_dict(key, value, size, dict_key)
        try:
            self._cache.set(key, dict_val)
        except Exception as e:
            self.__log.error("Error while setting the fixed size dict to cache. Error: {}".format(str(e)))

    def pushex(self, key, value, size, ttl, dict_key=None):
        """
        Pushes the value to a fixed size dictionary in storage with a time to live
        :return:
        """
        dict_val = self._append_to_dict(key, value, size, dict_key)
        try:
            self._cache.setex(key, dict_val, ttl)
        except Exception as e:
            self.__log.error("Error while setting the fixed size dict to cache. Error: {}".format(str(e)))

    def _append_to_dict(self, key, value, size, dict_key):
        """
        Appends the value to the FixedSize dictionary and pops the first item
        :param key:
        :param value:
        :param size:
        :return:
        """
        try:
            val = self._cache.get(key)
            self.__log.debug("Current value is {}".format(val))
            dict = FixedSizeDict(size=size)
            if val is not None:
                dict = dict.from_json(val)
            # ts = str(time.time())  # datetime.datetime.now()
            if dict_key is None:
                dict_key = time.time()
            dict[dict_key] = value
            return dict.to_json()
        except Exception as e:
            self.__log.error("Error while setting the fixed size dict to cache. Error: {}".format(str(e)))
            return None