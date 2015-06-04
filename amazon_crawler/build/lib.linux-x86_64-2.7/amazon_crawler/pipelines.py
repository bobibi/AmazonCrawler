# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import amazon_crawler.mysql_helper as db
from scrapy import log

class AmazonCrawlerPipeline(object):
    def process_item(self, item, spider):
        if not item['success']:
            print 'pipeline gets empty (success=False) item, ignored'
            log.msg('pipeline gets empty (success=False) item, ignored')
            return item
        elif spider.name == 'product':
            self.process_product(item['data'])
        elif spider.name == 'review':
            self.process_review(item['data'], item['page'])
        return item
    
    def process_review(self, data, page):
        for review in data:
            rev = db.AmazonReview()
            self.set_table_values(rev, review)
            try:
                db.insert_review(rev)
            except Exception, e:
                log.msg('insert review exception: %s'%str(e), level=log.WARNING)
        try:
            db.delete_review_page(data[0]['ASIN'], page)
        except Exception, e:
            log.msg('delete review page exception: %s'%str(e), level=log.WARNING)
        return
    
    def process_product(self, data):
        old_prod = db.get_product_by_asin(data['ASIN'])
        prod = db.AmazonProduct()
        self.set_table_values(prod, data)
        old_num_of_reviews = 0
        new_num_of_reviews = int(prod.NumberOfReviews)
        if old_prod:
            old_num_of_reviews = old_prod.NumberOfReviews
            db.update_product(prod)
        else:
            try:
                db.insert_product(prod)
            except Exception, e:
                log.msg("insert product exception: %s"%str(e), level=log.WARNING)
                return # ignore duplication
        '''now, update todo page table'''
        try:
            old_num_of_reviews < new_num_of_reviews and db.insert_review_page_list(prod.ASIN,
                                   range(old_num_of_reviews/10+1,\
                                         new_num_of_reviews/10+2 if new_num_of_reviews%10 else new_num_of_reviews/10+1))
        except  Exception, e:
            log.msg("insert review page exception: %s"%str(e), level=log.WARNING)
            return # ignore duplication    
    
    def set_table_values(self, tbl_obj, values):
        for name in values.keys():
            setattr(tbl_obj, name, values[name])
