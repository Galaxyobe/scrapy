# -*- coding: utf-8 -*-
from rpc import RPCClient
from scrapy import signals
import logging


class RPCProxyMiddleware(object):

    """ Proxy Middleware With RPC """

    def __init__(self, level):
        self.level = level
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        level = crawler.settings.get('PROXY_LIVE_LEVEL', 5)
        ext = cls(level)
        crawler.signals.connect(
            ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(
            ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        ip = getattr(spider, 'RPC_SERVER_IP', '127.0.0.1')
        port = getattr(spider, 'RPC_PORT', 4242)
        self.rpc = RPCClient(ip, port)
        self.client = self.rpc.getClient()
        # self.logger.info("opened spider %s", spider.name)

    def spider_closed(self, spider):
        self.rpc.close()

    def process_request(self, request, spider):
        proxyInfo = self.client.get(self.level)
        proxy = "http://%s:%s" % (proxyInfo['ip'], proxyInfo['port'])
        # Set the location of the proxy
        request.meta['proxy'] = proxy
        print 'proxy:', proxy

    # def process_response(self, request, response, spider):
    #     pass

    # def process_exception(self, request, exception, spider):
    #     pass
