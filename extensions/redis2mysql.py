# -*- coding: utf-8 -*-
from twisted.internet import task
from scrapy import signals
from scrapy2.utils.mysqlconnection import get_mysql_from_settings


class Redis2Mysql(object):

    """
    Redis data to Mysql
    """

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):

        self.crawler = crawler
        self.interval = 30
        self.use_set = crawler.settings.getbool('REDIS_START_URLS_AS_SET',
                                                False)

        cs = crawler.signals
        cs.connect(self.engine_started, signal=signals.engine_started)
        cs.connect(self.engine_stopped, signal=signals.engine_stopped)
        cs.connect(self.spider_opened, signal=signals.spider_opened)
        cs.connect(self.spider_closed, signal=signals.spider_closed)

    def engine_started(self):
        self.dbpool = get_mysql_from_settings(self.crawler.settings)

    def engine_stopped(self):
        self.dbpool.close()

    def spider_opened(self, spider):
        self.save = spider.server.sadd if self.use_set else spider.server.rpush
        self.fetch = spider.server.spop if self.use_set else spider.server.lpop
        self.task = task.LoopingCall(self.doing, spider)
        self.task.start(self.interval)

    def spider_closed(self, spider, reason):
        if self.task.running:
            self.task.stop()

    def doing(self, spider):
        while True:
            data = self.fetch(spider.key)
            if not data:
                break
            else:
                spider.logger.info('load:%s' % data)
