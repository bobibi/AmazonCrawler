# -*- coding: utf-8 -*-
import scrapy
import sys
from scrapy.selector import Selector 
import amazon_crawler.spider_logger as db_logger 
from amazon_crawler.items import AmazonItem as ReviewerItem
import amazon_crawler.mysql_helper as db
import amazon_crawler.html_extractor as html_extractor
from amazon_crawler.spider_base import SpiderBase
from cssselect import GenericTranslator, SelectorError
from scrapy import log
from scrapy import exceptions
import re


class ProductSpider(SpiderBase):
    name = "reviewer"
    html_page = 'AmazonReviewer'
    allowed_domains = ["amazon.com"]
    
    def __init__(self, *args, **kwargs):
        super(ProductSpider, self).__init__(args, kwargs)
        self.uid_list = super(ProductSpider, self).require_arg(args, kwargs, 'uid')
        self.url_template = super(ProductSpider, self).require_crawler_setting('UrlTemplate')
        
        self.start_urls = [re.sub('<<UID>>', a, self.url_template) for a in self.uid_list.split(',')]

    def parse(self, response):
        #super(ProductSpider, self).parse(response)
        item = ReviewerItem()
        if response.status != 200:
            db_log('url(%s) response code is %d, 200 is expected'%(response.url, response.status), 
                   lv='error', )
            item['success'] = False
            return item
        
        m = re.search('\/([0-9A-Z]{10,24})(?![0-9A-Z])', response.url)
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
            item['success'] = False
            item['message'] = 'some required fields are not extracted correctely due to missing selector, detail is in database'
        else:
            item['success'] = True
        
        reviewer = extract_result['data']
        reviewer[u'UID'] = uid
        
        item['data'] = reviewer
        item['debug'] = False
        if self.debug:
            item['debug'] = True
            
        return item
