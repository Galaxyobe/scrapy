# -*- coding: utf-8 -*-
from twisted.internet import defer
from twisted.internet.error import TimeoutError, DNSLookupError, \
    ConnectionRefusedError, ConnectionDone, ConnectError, \
    ConnectionLost, TCPTimedOutError
from twisted.web.client import ResponseFailed

from scrapy.utils.misc import load_object
from scrapy.exceptions import NotConfigured
from scrapy.utils.response import response_status_message
from scrapy.core.downloader.handlers.http11 import TunnelError


class DownloaderExceptionHandle(object):

    # IOError is raised by the HttpCompression middleware when trying to
    # decompress an empty response
    EXCEPTIONS_TO_HANDLE = (defer.TimeoutError, TimeoutError, DNSLookupError,
                            ConnectionRefusedError, ConnectionDone,
                            ConnectError, ConnectionLost, TCPTimedOutError,
                            ResponseFailed, IOError, TunnelError)

    def __init__(self, crawler):

        self.handle = crawler.settings.get('DOWNLOADEREXCEPTION_HANDLE')
        if self.handle:
            self.handle = load_object(self.handle)
        self.retry = crawler.settings.getbool('RETRY_ENABLED')
        self.max_retry_times = crawler.settings.getint('RETRY_TIMES')
        self.crawler = crawler
        self.settings = crawler.settings
        self.handle_http_codes = set(
            int(x) for x in crawler.settings.getlist('DOWNLOADEREXCEPTION_HANDLE_HTTP_CODES'))

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        if response.status in self.handle_http_codes:
            reason = response_status_message(response.status)
            return self._handle(request, reason, spider) or response
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, self.EXCEPTIONS_TO_HANDLE):
            return self._handle(request, exception, spider)

    def _handle(self, request, exception, spider):
        if self.retry:
            retries = request.meta.get('retry_times', 0) + 1
            if retries > self.max_retry_times:
                if self.handle:
                    return self.handle(request, exception, spider)
                if spider.downloader_exception_handle:
                    return spider.downloader_exception_handle(
                        request, exception, spider)
        else:
            if self.handle:
                return self.handle(request, exception, spider)
            if spider.downloader_exception_handle:
                return spider.downloader_exception_handle(request, exception,
                                                          spider)
