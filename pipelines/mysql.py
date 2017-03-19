# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.utils.misc import load_object
from scrapy.exceptions import NotConfigured
from scrapy2.utils.mysqlconnection import get_mysql_from_settings
from scrapy import signals


class MysqlPipeline(object):
    """
    Item into mysql
    """

    def __init__(self, dbpool, pipeline):
        self.dbpool = dbpool
        self.pipeline = pipeline

    @classmethod
    def from_settings(cls, settings):

        if settings.get('MYSQL_PIPELINE'):
            pipeline = load_object(settings['MYSQL_PIPELINE'])
        else:
            raise NotConfigured
        try:
            dbpool = get_mysql_from_settings(settings)
        except Exception as ex:
            print '%s \nException:%s' % (__file__, ex)
        return cls(dbpool, pipeline)

    @classmethod
    def from_crawler(cls, crawler):
        o = cls.from_settings(crawler.settings)
        crawler.signals.connect(
            o.engine_stopped, signal=signals.engine_stopped)
        return o

    def engine_stopped(self):
        self.dbpool.close()

    def process_item(self, item, spider):
        try:
            # run db query in the thread pool
            d = self.dbpool.runInteraction(self.pipeline, item, spider)
            d.addErrback(self._handle_error, item, spider)
            # at the end return the item in case of success or failure
            d.addBoth(lambda _: item)
            # return the deferred instead the item. This makes the engine to
            # process next item (according to CONCURRENT_ITEMS setting) after this
            # operation (deferred) has finished.
            return d
        except Exception as ex:
            print '%s process_item \nException:%s' % (__file__, ex)
        return item

    def _handle_error(self, failure, item, spider):
        """Handle occurred on db interaction."""
        # do nothing, just log
        spider.logger.err(failure)
