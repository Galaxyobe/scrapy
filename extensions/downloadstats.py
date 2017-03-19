# -*- coding: utf-8 -*-
from time import time

from scrapy.exceptions import NotConfigured
from twisted.internet import task
from scrapy import signals


class DownloadStats(object):

    """
    Scrapy crawl download stats

    ----------
        DOWNLOADSTATS_ENABLED default:True
                True: enable <DownloadStats> extension
                False: disabled it
        DOWNLOADSTATS_INTERVAL
                None or not defined it to disabled it
                float number interval recall
        DOWNLOADREQUEST_DISPLAY default:False
                True: dispaly down request
                False: not dispaly
    """

    def __init__(self, crawler):
        self.crawler = crawler
        self.interval = crawler.settings.getfloat('DOWNLOADSTATS_INTERVAL')
        self.display = crawler.settings.getbool(
            'DOWNLOADREQUEST_DISPLAY', False)
        if not self.interval:
            raise NotConfigured
        self.save_succeed = crawler.settings.getbool('DOWNLOADSUCCEED_SAVE',
                                                     False)
        cs = crawler.signals
        cs.connect(self.spider_opened, signal=signals.spider_opened)
        cs.connect(self.spider_closed, signal=signals.spider_closed)
        cs.connect(self.request_scheduled, signal=signals.request_scheduled)
        cs.connect(self.response_received, signal=signals.response_received)
        cs.connect(self.item_scraped, signal=signals.item_scraped)

        self.latency, self.proc_latency, self.items = 0, 0, 0

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('DOWNLOADSTATS_ENABLED', True):
            raise NotConfigured
        return cls(crawler)

    def spider_opened(self, spider):
        self.task = task.LoopingCall(self.log, spider)
        self.task.start(self.interval)

    def spider_closed(self, spider, reason):
        if self.task.running:
            self.task.stop()

    def request_scheduled(self, request, spider):
        request.meta['DownloadStats_ScheduleTimeStamp'] = time()

    def response_received(self, response, request, spider):
        request.meta['DownloadStats_ReceivedTimeStamp'] = time()
        if self.display:
            if response.meta.get('DownloadStats_ScheduleTimeStamp'):
                latency = time() - response.meta.get(
                    'DownloadStats_ScheduleTimeStamp')
                spider.logger.info('%s [%.3fs]' % (request, latency))

    def item_scraped(self, item, response, spider):
        self.latency += time() - response.meta.get(
            'DownloadStats_ScheduleTimeStamp')
        self.proc_latency += time() - response.meta.get(
            'DownloadStats_ReceivedTimeStamp')
        self.items += 1

    def log(self, spider):
        irate = float(self.items) / self.interval
        latency = self.latency / self.items if self.items else 0
        proc_latency = self.proc_latency / self.items if self.items else 0

        spider.logger.info(("Scraped %d items at %.1f items/s, avg latency: "
                            "%.3f s and avg time in pipelines: %.3f s") %
                           (self.items, irate, latency, proc_latency))

        self.latency, self.proc_latency, self.items = 0, 0, 0
