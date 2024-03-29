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
    debug = False
    def __init__(self, *args, **kwargs):
        if kwargs.has_key('debug') and kwargs.get('debug').lower()=='on':
            self.debug = True
    
    def require_arg(self, name, *args, **kwargs):
        '''
        This method will get argument from command line which is provided by using "-a arg=val"
        If the argument is not found, an exception is raised
        It will retrieve the regexp from database setting table, and use it to match the coming arg
        If the matching failed, an exception will be raised
        If no regexp found, exception too
        '''
        arg_regexp = self.require_crawler_setting('ClArg.'+name+'.Regexp')
        if not kwargs.has_key(name) or not re.match(arg_regexp, kwargs.get(name)):
            log.msg('missing or invalid param %s'%name, level=log.ERROR)
            raise exceptions.CloseSpider('missing or invalid param %s, please use "-a %s=<<%s>>"'%(name,name,name))
        return kwargs.get(name)
        
    def require_crawler_setting(self, name):
        ans = db.get_crawler_setting(self.html_page, name)
        if ans == None:
            log.msg('cannot find %s in crawler setting from database'%name, level=log.ERROR)
            raise exceptions.CloseSpider('cannot find %s in crawler setting from database'%name)
        return ans
