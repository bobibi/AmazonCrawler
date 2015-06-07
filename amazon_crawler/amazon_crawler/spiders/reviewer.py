# -*- coding: utf-8 -*-
import scrapy
import sys
from scrapy.selector import Selector 
import amazon_crawler.spider_logger as db_logger 
from amazon_crawler.items import AmazonItem as ReviewerItem
import amazon_crawler.mysql_helper as db
import amazon_crawler.html_extractor as html_extractor
from cssselect import GenericTranslator, SelectorError
from scrapy import log
from scrapy import exceptions
import re


class ProductSpider(scrapy.Spider):
    name = "reviewer"
    html_page = 'AmazonReviewer'
    allowed_domains = ["amazon.com"]
    
    def __init__(self, *args, **kwargs):
        if not kwargs.has_key('uid') or not re.match('^[0-9A-Z]{13,15}(,[0-9A-Z]{13,15})*$', kwargs.get('uid')):
            log.msg('missing or invalid param uid', level=log.ERROR)
            raise exceptions.CloseSpider('missing or invalid param uid, please use "-a uid=<<Amazon User ID>>"')
        self.url_template = db.get_crawler_setting(self.html_page, 'UrlTemplate')
        if not self.url_template:
            log.msg('cannot find url template in crawler setting from database', level=log.ERROR)
            raise exceptions.CloseSpider('cannot find url template in crawler setting from database')
        self.start_urls = [re.sub('<<UID>>', a, self.url_template) for a in kwargs.get('uid').split(',')]

    def parse(self, response):
        item = ReviewerItem()
        if response.status != 200:
            db_log('url(%s) response code is %d, 200 is expected'%(response.url, response.status), 
                   lv='error', )
            item['success'] = False
            return item
        
        m = re.search('\/([0-9A-Z]{13,15})(?![0-9A-Z])', response.url)
        if not m:
            db_log('cannot parse uid from response url: %s'%response.url, lv='error', spider=self.name)
            log.msg('cannot parse uid from response url: %s'%response.url, level=log.ERROR)
            raise exceptions.CloseSpider('cannot parse uid from response url:%s'%response.url)
        uid = m.group(1)
        
        sel = Selector(response)
        extractor_list = db.get_page_extractor_list(self.html_page)
        if not extractor_list:
            db_log(message = 'no extractor for Page=%s, refer to table HtmlExtractor'%self.html_page,
                   lv = 'fatal',spider = self.name)
        
        extract_result = html_extractor.extract(sel, extractor_list, self.name, uid)
        
        if extract_result['mismatch']:
            raise exceptions.CloseSpider('some required fields are not extracted correctely due to missing selector, detail is in database')
        
        reviewer = extract_result['data']
        reviewer[u'UID'] = uid
        reviewer[u'URL'] = response.url
        
        item['data'] = reviewer
        item['success'] = True
        return item
