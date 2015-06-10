from scrapy.selector import Selector 
import amazon_crawler.spider_logger as db_logger 
import amazon_crawler.mysql_helper as db
from cssselect import GenericTranslator, SelectorError
import re
import datetime
from scrapy import log

def extract(sel, extractor_list, spider, params): # spider and params are used for context specifying
    db_log = db_logger.log
    ans = {}
    mismatch = False
    for extractor in extractor_list:
        log.msg('##START##Extracting %s.%s' % (extractor.Page, extractor.Field))
        css_selector_list = db.get_field_selector_list(extractor.Page, extractor.Field)
        field_value = None
        if not css_selector_list:
            db_log(message = 'no selector available for P='+extractor.Page+', F='+extractor.Field+', refer to table HtmlSelector',
                   lv = 'fatal',spider = spider)
            ## fatal will terminate program
        for css_selector in css_selector_list:
            try:
                if css_selector.Type == 'css':
                    if not css_selector.XpathConvertable:
                        continue
                    log.msg('Attempting css: %s' % css_selector.Selector)
                    expression = GenericTranslator().css_to_xpath(css_selector.Selector)                
                    if not css_selector.Reader or css_selector.Reader == 'text':
                        expression += '/text()'
                    elif css_selector.Param:
                        expression += '/@'+css_selector.Param
                    else:
                        db_log('invalid entry in table HtmlSelector, Page='
                                +css_selector.Page+',Field='
                                +css_selector.Field+',Selector='+css_selector.Selector,
                                'error')
                        continue
                elif css_selector.Type == 'xpath':
                    expression = css_selector.Selector
                else:
                    db_log('unknown selector type: %s'%css_selector.Type, 'warning', spidder=spider, params=params)
                    continue
                        
                log.msg('Xpath: %s' % expression)
                match = sel.xpath(expression).extract()
                if match: 
                    ''' selector works '''
                    value = match[0]
                    log.msg('Extracted: %s' % value)
                    log.msg('Validator: %s' % extractor.PyValidator)
                    m = re.search(extractor.PyValidator, value)
                    if not m:
                        continue
                    ''' regexp validation passed '''
                    field_value = m.group(1)
                    log.msg('After validated: %s' % field_value)
                    break                        
            except SelectorError,e:
                '''db_log('css2xpath error,'+str(e)+',P='+css_selector.Page+',F='+css_selector.Field+',S='+css_selector.Selector,
                       'warning')'''
                log.msg('css2xpath error,'+str(e)+',P='+css_selector.Page+',F='+css_selector.Field+',S='+css_selector.Selector,
                       log.WARNING)
                
        if not field_value and extractor.Required:
            mismatch = True
            db_log('mismatch field for all selector, P=%s, F=%s'%(extractor.Page, extractor.Field), 
                   'error', spider=spider, params=params)
        elif field_value != None:
            if extractor.PyFilter:
                if not validate_filter(extractor.PyFilter):
                    db_log('invalid filter (T=HtmlExtractor,P=%s,F=%s), a lambda expression is expected: "value=(lambda ...)(value)"'\
                           %(extractor.Page,extractor.Field),
                           'fatal')
                value = field_value
                try:
                    exec(extractor.PyFilter)
                    log.msg('After Filtered: %s' % value)
                except Exception,e:
                    db_log('filter exception (T=HtmlExtractor,P=%s,F=%s): %s'%(extractor.Page,extractor.Field,str(e)),
                           'fatal')
                field_value = value
        if field_value != None:
            log.msg('##EXTRACTED## %s=%s'%(extractor.Field,field_value))
        if field_value == None and extractor.Default != None:
            field_value = extractor.Default
        ans[extractor.Field] = field_value
    return {'mismatch': mismatch, 'data':ans}

def validate_filter(filter):
    return True if re.match('^value\s*=\s*\(\s*lambda\s+.+\)\s*\(\s*value\s*\)\s*$', filter) else False

