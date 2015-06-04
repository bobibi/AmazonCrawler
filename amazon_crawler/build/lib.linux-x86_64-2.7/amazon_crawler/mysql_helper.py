'''
Database Connection Helper Module
'''
from sqlalchemy import MetaData, Integer, Table, Column, text
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, exc

engine = create_engine("mysql://ReviewSec:ecbenest@localhost/ReviewSec?charset=utf8&use_unicode=1", echo=False)
Base = declarative_base()
metadata = MetaData(bind=engine)
session = sessionmaker()
session.configure(bind=engine)
s = session()

''' Table Classes
'''
class AmazonProduct(Base):
    __table__=Table('AmazonProduct', metadata, autoload=True)
    
class AmazonReview(Base):
    __table__=Table('AmazonReview', metadata, autoload=True)

class AmazonReviewPage(Base):
    __table__=Table('AmazonReviewPage', metadata, autoload=True)

class HtmlExtractor(Base):
    __table__=Table('HtmlExtractor', metadata, autoload=True)

class HtmlSelector(Base):
    __table__=Table('HtmlSelector', metadata, autoload=True)
    
class SpiderLog(Base):
    __table__=Table('SpiderLog', metadata, autoload=True)

 
''' DB Access Helper Functions
'''
def get_product_by_asin(prod_asin):
    global s
    try:
        prod = s.query(AmazonProduct).filter(AmazonProduct.ASIN == prod_asin).one()
    except exc.NoResultFound:
        prod = None
    return prod

def update_product(prod):
    ''' Be careful, this function only updates the NumberOfReviews column!!!'''
    global s
    try:
        s.query(AmazonProduct).filter(AmazonProduct.ASIN == prod.ASIN).update({"NumberOfReviews": prod.NumberOfReviews})
        s.commit()
    except:
        s.rollback()
        raise
    
def insert_product(prod):
    global s
    try:
        s.add(prod)
        s.commit()
    except:
        s.rollback()
        raise
    
def insert_review_page_list(asin, page_list):
    global s
    for page in page_list:
        try:
            s.add(AmazonReviewPage(ASIN=asin, PageNumber=page))
            s.commit()
        except:
            s.rollback()
            raise

def get_html_selector_by_page_field(page, field):
    global s
    try:
        selector = s.query(HtmlSelector).filter(HtmlSelector.Page==page, 
                                                        HtmlSelector.Field==field).all()
    except exc.NoResultFound:
        selector = None
    return selector

def get_field_list(page):
    global s
    try:
        flist = s.query(HtmlExtractor, HtmlExtractor.Field).filter(HtmlExtractor.Page==page,
                                                                           HtmlExtractor.Disabled=='0').all()
    except exc.NoResultFound:
        flist = None
    return flist

def get_page_extractor_list(page):
    global s
    try:
        flist = s.query(HtmlExtractor).filter(HtmlExtractor.Page==page, HtmlExtractor.Disabled=='0').all()
    except exc.NoResultFound:
        flist = None
    return flist

def get_field_selector_list(page, field):
    global s
    try:
        sellist = s.query(HtmlSelector).filter(HtmlSelector.Page==page, HtmlSelector.Field==field).all()
    except exc.NoResultFound:
        sellist = None
    return sellist

def insert_review(review):
    global s
    try:
        s.add(review)
        s.commit()
    except:
        s.rollback()
        raise
    
def delete_review_page(asin, page):
    global s
    try:
        rp = s.query(AmazonReviewPage).filter(AmazonReviewPage.ASIN==asin, AmazonReviewPage.PageNumber==page).one()
        s.delete(rp)
        s.commit()
    except:
        s.rollback()
        raise


