# -*- coding: utf-8 -*-
import scrapy
import sys
from scrapy.selector import Selector 
import amazon_crawler.spider_logger as db_logger 
from amazon_crawler.items import AmazonItem as ProductItem
import amazon_crawler.mysql_helper as db
import amazon_crawler.html_extractor as html_extractor
from cssselect import GenericTranslator, SelectorError
from scrapy import log
from scrapy import exceptions
import re


class ProductSpider(scrapy.Spider):
    name = "product"
    html_page = 'AmazonProduct'
    allowed_domains = ["amazon.com"]
    
    def __init__(self, *args, **kwargs):
        if not kwargs.has_key('asin') or not re.match('^[0-9A-Z]{10}(,[0-9A-Z]{10})*$', kwargs.get('asin')):
            log.msg('missing or invalid param asin', level=log.ERROR)
            raise exceptions.CloseSpider('missing or invalid param asin, please use "-a asin=<<ASIN>>"')
        self.url_template = db.get_crawler_setting('AmazonProduct', 'UrlTemplate')
        if not self.url_template:
            log.msg('cannot find url template in crawler setting from database', level=log.ERROR)
            raise exceptions.CloseSpider('cannot find url template in crawler setting from database')
        self.start_urls = [re.sub('<<ASIN>>', a, self.url_template) for a in kwargs.get('asin').split(',')]

    def parse(self, response):
        item = ProductItem()
        if response.status != 200:
            db_log('url(%s) response code is %d, 200 is expected'%(response.url, response.status), 
                   lv='error', )
            item['success'] = False
            return item
        
        m = re.search('\/([0-9A-Z]{10})(\/|$|\?)', response.url)
        if not m:
            db_log('cannot parse asin from response url: %s'%response.url, lv='error', spider=self.name)
            log.msg('cannot parse asin from response url: %s'%response.url, level=log.ERROR)
            raise exceptions.CloseSpider('cannot parse asin from response url:%s'%response.url)
        asin = m.group(1)
        
        sel = Selector(response)
        extractor_list = db.get_page_extractor_list(self.html_page)
        if not extractor_list:
            db_log(message = 'no extractor for Page=%s, refer to table HtmlExtractor'%self.html_page,
                   lv = 'fatal',spider = self.name)
        
        extract_result = html_extractor.extract(sel, extractor_list, self.name, asin)
        
        if extract_result['mismatch']:
            item['message'] = 'some required fields are not extracted correctely due to missing selector, detail is in database'
            item['success'] = False
        else:
            item['success'] = True
        
        prod = extract_result['data']
        prod[u'ASIN'] = asin
        prod[u'URL'] = response.url
        
        item['data'] = prod
        return item
