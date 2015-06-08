# -*- coding: utf-8 -*-
import scrapy
import sys
from scrapy.selector import Selector 
import amazon_crawler.spider_logger as db_logger 
import amazon_crawler.mysql_helper as db
import amazon_crawler.html_extractor as html_extractor
from cssselect import GenericTranslator, SelectorError
from scrapy import log
from scrapy import exceptions
import re
from abc import ABCMeta

class SpiderBase(scrapy.Spider):
    __metaclass__ = ABCMeta
    
    def get_arg(self, *args, **kwargs, name):
        arg_setting = db.get_crawler_setting(self.html_page, name)
        if not arg_setting:
            log.msg('missing configuration setting for arg: %s, refer to db table CrawlerSetting'%name, level=log.ERROR)
            raise exceptions.CloseSpider('missing configuration setting for arg: %s, refer to db table CrawlerSetting'%name)
        if not kwargs.has_key(name) or not re.match(arg_setting.Value, kwargs.get(name)):
            log.msg('missing or invalid param %s'%name, level=log.ERROR)
            raise exceptions.CloseSpider('missing or invalid param %s, please use "-a %s=<<%s>>"'%(name,name,name))
        setattr(self, name, kwargs.get(name))