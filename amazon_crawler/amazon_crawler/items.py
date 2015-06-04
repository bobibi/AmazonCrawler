# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import amazon_crawler.mysql_helper as db
import amazon_crawler.spider_logger as db_logger
import scrapy
import sys


class AmazonItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    data = scrapy.Field()
    success = scrapy.Field()
    
class ReviewPageItem(AmazonItem):
    # define the fields for your item here like:
    # name = scrapy.Field()
    page = scrapy.Field()
    numberofreviews = scrapy.Field()
