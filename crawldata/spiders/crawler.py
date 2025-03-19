import scrapy,json, requests, urllib3
from crawldata.functions import *
from datetime import datetime
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CrawlerSpider(scrapy.Spider):
    name = 'grimme'
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #custom_settings={'LOG_FILE':'./log/'+name+'_'+DATE_CRAWL+'.log'}
    domain='https://www.shop.grimme.co.uk'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0','Accept': '*/*','Accept-Language': 'en-US,en;q=0.5','Referer': domain+'/en/search','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin','Priority': 'u=4'}
    def start_requests(self):
        yield scrapy.Request(self.domain+'/en/machine-finder/ajax/search?result-type=technology',callback=self.parse_data,headers=self.headers,dont_filter=True)
    def parse_data(self,response):
        Data=json.loads(response.text)
        for item in Data['data']:
            item['brand']='Grimme'
            item['tech_name']=item['name']
            del item['name']
            RES=requests.Session()
            url=self.domain+'/en/machine-finder/ajax/search?filter%5Btechnology%5D%5BidMachineTechnology%5D='+str(item['idMachineTechnology'])+'&result-type=product-group'
            response=RES.get(url,verify=False)
            Data=json.loads(response.text)
            for row in Data['data']:
                item['idMachineProductGroup']=row['idMachineProductGroup']
                item['machine_name']=row['name']
                url=self.domain+'/en/machine-finder/ajax/search?filter%5BproductGroup%5D%5BidMachineProductGroup%5D='+str(item['idMachineProductGroup'])+'&filter%5Btechnology%5D%5BidMachineTechnology%5D='+str(item['idMachineTechnology'])+'&result-type=series-year'
                response=RES.get(url,verify=False)
                Data=json.loads(response.text)
                for year in Data['data']:
                    item['year']=year
                    url=self.domain+'/en/machine-finder/ajax/search?filter%5BmachineSeries%5D%5BmanufacturingYear%5D='+str(item['year'])+'&filter%5BproductGroup%5D%5BidMachineProductGroup%5D='+str(item['idMachineProductGroup'])+'&filter%5Btechnology%5D%5BidMachineTechnology%5D='+str(item['idMachineTechnology'])+'&result-type=series'
                    response=RES.get(url,verify=False)
                    Data=json.loads(response.text)
                    for row in Data['data']:
                        item.update(row)
                        url=self.domain+'/en/machine-or-series/ajax?machine-series-id='+str(item['idMachineSeries'])
                        response=RES.get(url,verify=False)
                        Data=json.loads(response.text)
                        for row in Data['data']:
                            item.update(row)
                            json_data = {'machine-series-id': item['idMachineSeries']}
                            url=self.domain+'/en/preferences'
                            response=RES.post(url,verify=False,json=json_data)
                            # Get category link
                            url=self.domain+'/en/search'
                            resp=RES.get(url,verify=False)
                            response=scrapy.Selector(text=resp.text)
                            Data=response.xpath('//div[@class="filter-section__item"]/filter-category/ul/li')
                            CATES={}
                            for row in Data:
                                CATES=get_cat(CATES,'','',row)
                            for k,v in CATES.items():
                                item['categories']=k
                                url=self.domain+v+'?ipp=36&page=1'
                                RUN=True
                                while RUN:
                                    resp=RES.get(url,verify=False)
                                    response=scrapy.Selector(text=resp.text)
                                    Data=response.xpath('//product-item')
                                    for row in Data:
                                        link=row.xpath('.//a/@href').get()
                                        item['url']=self.domain+link
                                        img=row.xpath('.//a[@class="product-item__image-container"]/img/@src').get()
                                        if str(img).startswith('http'):
                                            item['image']=img
                                        else:
                                            item['image']=self.domain+img
                                        # Get product content
                                        resp=RES.get(item['url'],verify=False)
                                        response=scrapy.Selector(text=resp.text)
                                        itemdt={}
                                        itemdt['brand']=item['brand']
                                        itemdt['category']=str(item['categories']).split(" ~ ")[0]
                                        itemdt['subcategory']=''
                                        if ' ~ ' in item['categories']:
                                            itemdt['subcategory']=str(item['categories']).split(" ~ ")[1]
                                        itemdt['part_number']=response.xpath('//div[@class="product-configurator__top-block"]/div[contains(@class,"text--grey")]/text()').get()
                                        itemdt['sku']=itemdt['brand']+"-"+itemdt['part_number']
                                        itemdt['name']=response.xpath('//div[@class="product-configurator__top-block"]/h1/text()').get()
                                        itemdt['price']=response.xpath('//h4[@itemprop="price"]/@content').get()
                                        itemdt['description']=response.xpath('//h6[text()="Description"]/../div[@class="text__body1"]/text()').get()
                                        if itemdt['description']:
                                            itemdt['description']=str(itemdt['description']).strip()
                                        itemdt['image']=item['image']
                                        itemdt['fitment_data']=item['name']+" ("+item['model']+")"
                                        itemdt['equipment_fit']=item['tech_name']+" -> "+item['machine_name']+" -> "+item['year']+" -> "+item['model']
                                        itemdt['id']=str(item['idMachineTechnology'])+"-"+str(item['idMachineProductGroup'])+"-"+item['year']+"-"+str(item['idMachineSeries'])+"-"+str(itemdt['part_number']).replace(" ","_")
                                        itemdt['url']=item['url']
                                        itemdt['cat_id']=key_MD5(itemdt['sku']+itemdt['category']+itemdt['subcategory'])
                                        # itemdt['compatiable']=response.xpath('//div[contains(@class,"js-product-configurator__machine-selection")]/text()').get().strip()
                                        imgs=response.xpath('//div[contains(@class,"image-gallery__gallery--items")]//img/@src').getall()
                                        itemdt['additional_images']=[]
                                        if len(imgs)>1:
                                            for i in range(1,len(imgs)):
                                                img=imgs[i]
                                                if str(img).startswith('http'):
                                                    itemdt['additional_images'].append(img)
                                                else:
                                                    itemdt['additional_images'].append(self.domain+img)
                                        itemdt['dimensions']={}
                                        itemdt['weight']={}
                                        if itemdt['description'] and '|' in itemdt['description']:
                                            OPTIONS=str(itemdt['description']).split('|')
                                            for option in OPTIONS:
                                                rcs=option.split(':')
                                                if len(rcs)>1:
                                                    key=(str(rcs[0]).strip()).replace('Ø','')
                                                    if str(key).lower()=='weight':
                                                        itemdt['weight']['value']=str(rcs[1]).strip()
                                                    else:
                                                        itemdt['dimensions'][key]=str(rcs[1]).strip()

                                        yield(itemdt)
                                    if len(Data)>=36:
                                        url=str(url)[:-1]+str(int(str(url)[-1:])+1)
                                    else:
                                        RUN=False
