# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.utils.misc import load_object
from scrapy.utils.serialize import ScrapyJSONEncoder
from twisted.internet.threads import deferToThread
from scrapy_redis import connection, defaults

default_serialize = ScrapyJSONEncoder().encode
""" Redis Pipeline """


class RedisPipeline(object):
    """
    Pushes serialized item into a redis set

    Settings
    --------
    REDIS_ITEMS_KEY : str
        Redis key where to store items.
    REDIS_ITEMS_SERIALIZER : str
        Object path to serializer function.

    """

    def __init__(self,
                 server,
                 key=defaults.PIPELINE_KEY,
                 serialize_func=default_serialize,
                 item_as_set=False):
        """
        Initialize pipeline.

        Parameters
        ----------
        server : StrictRedis
            Redis client instance.
        key : str
            Redis key where to store items.
        serialize_func : callable
            Items serializer function.

        """
        self.server = server
        self.key = key
        self.serialize = serialize_func
        self.save = self.server.sadd if item_as_set else self.server.rpush

    @classmethod
    def from_settings(cls, settings):
        params = {
            'server': connection.from_settings(settings),
        }
        if settings.get('REDIS_ITEMS_KEY'):
            params['key'] = settings['REDIS_ITEMS_KEY']
        if settings.get('REDIS_ITEMS_SERIALIZER'):
            params['serialize_func'] = load_object(
                settings['REDIS_ITEMS_SERIALIZER'])
        if settings.get('REDIS_ITEM_AS_SET'):
            params['item_as_set'] = settings['REDIS_ITEM_AS_SET']
        return cls(**params)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        key = self.item_key(item, spider)
        data = self.serialize(item)
        self.save(key, data)
        return item

    def item_key(self, item, spider):
        """
        Returns redis key based on given spider.

        Override this function to use a different key depending on the item
        and/or spider.

        """
        return self.key % {'spider': spider.name}
