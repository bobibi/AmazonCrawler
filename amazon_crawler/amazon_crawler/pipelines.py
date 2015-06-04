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
            self.process_review(item['data'], item['page'], item['numberofreviews'])
        return item
    
    def process_review(self, data, page, numberofreviews):
        seq_from = numberofreviews-10*(page-1)
        seq_to = max(seq_from-9, 1)
        seqs = range(seq_from, seq_to-1, -1)
        for i in range(0, len(seqs)):
            review = data[i]
            review['SeqNo'] = seqs[i]
            rev = db.AmazonReview()
            self.set_table_values(rev, review)
            try:
                db.insert_review(rev)
            except Exception, e:
                log.msg('insert review exception: %s'%str(e), level=log.WARNING)
        try:
            db.insert_review_crawled_log(data[0]['ASIN'], seq_to, seq_from)
        except Exception, e:
            log.msg('insert review crawled log exception: %s'%str(e), level=log.WARNING)
    
    def process_product(self, data):
        '''1) get old prod info from db, 2) update prod or insert prod'''
        old_prod = db.get_product_by_asin(data['ASIN'])
        prod = db.AmazonProduct()
        self.set_table_values(prod, data)
        if old_prod:
            try:
                db.update_product(prod)
            except Exception, e:
                log.msg("update product exception: %s"%str(e), level=log.WARNING)
                return # ignore update error
        else:
            try:
                db.insert_product(prod)
            except Exception, e:
                log.msg("insert product exception: %s"%str(e), level=log.WARNING)
                return # ignore duplication
    
    def set_table_values(self, tbl_obj, values):
        for name in values.keys():
            setattr(tbl_obj, name, values[name])
