# -*- coding: utf-8 -*-

# Scrapy settings for amazon_crawler project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'amazon_crawler'

SPIDER_MODULES = ['amazon_crawler.spiders']
NEWSPIDER_MODULE = 'amazon_crawler.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'amazon_crawler (+http://www.yourdomain.com)'

ITEM_PIPELINES = ['amazon_crawler.pipelines.AmazonCrawlerPipeline']
LOG_LEVEL = 'DEBUG'
COOKIES_DEBUG = False
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 Safari/537.36'
