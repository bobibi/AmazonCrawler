# -*- coding: utf-8 -*-
import scrapy
import sys
import re
from scrapy.selector import Selector 
import amazon_crawler.spider_logger as db_logger 
from amazon_crawler.items import ReviewPageItem as ReviewItem
import amazon_crawler.mysql_helper as db
import amazon_crawler.html_extractor as html_extractor
from cssselect import GenericTranslator, SelectorError
from scrapy import log
from scrapy import exceptions

db_log = db_logger.log

class ReviewSpider(scrapy.Spider):
    name = "review"
    html_page = 'AmazonReview'
    allowed_domains = ["amazon.com"]
     
    def __init__(self, *args, **kwargs):
        if not kwargs.has_key('asin') or not re.match('^[0-9A-Z]{10}$', kwargs.get('asin')):
            log.msg('missing or invalid param asin', level=log.ERROR)
            raise exceptions.CloseSpider('missing param asin, please use "-a asin=<<ASIN>>"')
        if not kwargs.has_key('page') or not re.match('^[1-9]\d*(,[1-9]\d*)*$', kwargs.get('page')):
            log.msg('missing or invalid param page', level=log.ERROR)
            raise exceptions.CloseSpider('missing or invalid param page, please use "-a page=<<PAGE>>"')
        
        self.url_template = db.get_crawler_setting(self.html_page, 'UrlTemplate')
        if not self.url_template:
            log.msg('cannot find url template in crawler setting from database', level=log.ERROR)
            raise exceptions.CloseSpider('cannot find url template in crawler setting from database')
        self.asin = kwargs.get('asin')
        url = re.sub('<<ASIN>>', self.asin, self.url_template)
        self.page_list = [int(i) for i in kwargs.get('page').split(',')]        
        self.start_urls = [re.sub('<<PAGE>>', str(p), url) for p in self.page_list] 

    def parse(self, response):
        item = ReviewItem()
        item['success'] = True
        if response.status != 200:
            db_log('url(%s) response code is %d, 200 is expected'%(response.url, response.status), 
                   lv='error', )
            item['success'] = False
            return item
            
        m = re.search('\/([0-9A-Z]{10})(\/|$|\?)', response.url)
        if not m:
            log.msg('cannot parse asin from response url: %s'%response.url, level=log.ERROR)
            db_log('cannot parse asin from response url: %s'%response.url, lv='error', spider=self.name)
            item['success'] = False
            return item
        asin = m.group(1)
        m = re.search('pageNumber=(\d+)', response.url)
        if not m:
            log.msg('cannot find page number from response url: %s'%response.url, level=log.ERROR)
            db_log('cannot find page number from response url: %s'%response.url, lv='error', spider=self.name)
            item['success'] = False
            return item
        item['page'] = int(m.group(1))
        page = item['page']
        
        sel = Selector(response)        
        wrapper_selector_list = db.get_html_selector_by_page_field(self.html_page, 'wrapper')
        if not wrapper_selector_list:
            db_log('no wrapper available for %s in table HtmlSelector'%self.html_page, 'fatal',spider = self.name)
        extractor_list = db.get_page_extractor_list(self.html_page)
        if not extractor_list:
            db_log(message = 'no extractor for Page=AmazonProduct, refer to table HtmlExtractor',lv = 'fatal',spider = self.name)
        total_review_count = sel.xpath('//span[contains(@class,"totalReviewCount")]/text()').extract()
        if not total_review_count:
            db_log('cannot find totalReviewCount from response, url: %s'%response.url, lv='fatal', spider=self.name)
        item['numberofreviews'] = int(re.sub(',','',total_review_count[0]))
        
        extract_result = None
        for wrapper_selector in wrapper_selector_list:
            try:
                log.msg('Wrapper css: %s' % wrapper_selector.Selector, log.DEBUG)
                expression = GenericTranslator().css_to_xpath(wrapper_selector.Selector)
                log.msg('Wrapper xpath: %s' % expression, log.DEBUG)
                wrapper = sel.xpath(expression)
                if wrapper:
                    extract_result = [html_extractor.extract(w, extractor_list, self.name, '%s[%d]'%(asin,page)) for w in wrapper]

                    if sum([0 if i['mismatch'] else 1 for i in extract_result]) > 0: # at least one review being parsed correctly
                        break
                    
            except SelectorError,e:
                log.msg('css2xpath error,'+str(e)+',P='+wrapper_selector.Page+',F='+wrapper_selector.Field+',S='+wrapper_selector.Selector,
                       log.WARNING)            
          
        if not hasattr(extract_result, "__iter__") or sum([0 if i['mismatch'] else 1 for i in extract_result]) == 0:
            item['success'] = False
            return item

        item['data'] = [dict({'ASIN': asin}, **i) for i in extract_result]

        return item
