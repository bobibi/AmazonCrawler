import mysql_helper as db
import sys
from scrapy import exceptions

level = {'debug': 0, 'warning': 1, 'error': 2, 'fatal': 9}

def log(message, lv='debug', spider='none', params='none'):
    global level
    s = db.s
    log = db.SpiderLog(Spider=spider,
                       ParamList=params,
                       Level=level[lv],
                       Message=message)
    s.add(log)
    s.commit()
    
    if lv=='fatal':
        raise exceptions.CloseSpider('fatal error encountered, please check log in database')