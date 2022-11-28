import scrapy
from scrapy.crawler import CrawlerProcess
import json
import requests
import openpyxl
import  os

file_data= open('all_urls.txt','r')
previous_urls= [v.strip() for v in file_data.readlines()]


file = open('all_urls.txt','a')



pre_seed = []
seed = []
series_A = []
series_B = []
Location = []
under_represented_founder = []

work_sheet = openpyxl.load_workbook('Meta.xlsx')

for cell in work_sheet['Sheet1']:
    try:
        try:
            value = cell[0].hyperlink.target
        except:
            pass
        try:
            value1 = cell[1].hyperlink.target
        except:
            pass
        try:
            value2 = cell[2].hyperlink.target
        except:
            pass
        try:
            value3 = cell[3].hyperlink.target
        except:
            pass
        try:
            value4 = cell[4].hyperlink.target
        except:
            pass
        try:
            value5 = cell[5].hyperlink.target
        except:
            pass
        try:
            if (value!=None ) or (value!=''):
                pre_seed.append(value)
        except:
            pass

        try:
            if (value1!=None ) or (value1!=''):
                seed.append(value1)
        except:
            pass

        try:
            if (value2!=None ) or (value2!=''):
                series_A.append(value2)
        except:
            pass
        try:
            if (value3!=None ) or (value3!=''):
                series_B.append(value3)
        except:
            pass
        try:
            if (value4!=None ) or (value4!=''):
                Location.append(value4)
        except:
            pass

        try:
            if (value5!=None ) or (value5!=''):
                under_represented_founder.append(value5)
        except:
            pass
    except AttributeError:
        pass

main_list = []

cwd = os.getcwd()
class investor(scrapy.Spider):
    name = 'investor'
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 2,
        'DOWNLOAD_TIMEOUT': 10,
        'RETRY_TIMES': 30,
        'RETRY_HTTP_CODES': [302, 503, 400, 403],
        # 'Handle_httpstatus_list': [400, 403],
        'ROTATING_PROXY_LIST_PATH': f'{cwd}/proxy.txt',
        'DOWNLOADER_MIDDLEWARES': {
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620
        },
    }

    def start_requests(self):
        for prese in list(set(pre_seed)):
            yield scrapy.Request(url=prese)
        for sed in list(set(seed)):
            yield scrapy.Request(url=sed)
        for serA in list(set(series_A)):
            yield scrapy.Request(url=serA)

        for serB in list(set(series_B)):
            yield scrapy.Request(url=serB)
        for loc in list(set(Location)):
            yield scrapy.Request(url=loc)

        for unrefo in list(set(under_represented_founder)):
            yield scrapy.Request(url=unrefo)


    def parse(self,response):

        p_see = response.css('div#stage-pre_seed p.f6.ttu.fw6 ::text').get()
        if p_see == None:
            p_see = ''

        s_see = response.css('div#stage-seed p.f6.ttu.fw6 ::text').get()
        if s_see == None:
            s_see = ''

        s_see_A = response.css('div#stage-series_a p.f6.ttu.fw6 ::text').get()
        if s_see_A == None:
            s_see_A = ''

        s_see_B = response.css('div#stage-series_b p.f6.ttu.fw6 ::text').get()
        if s_see_B == None:
            s_see_B = ''


        if ('Pre-Seed' in p_see) or ('Seed' in s_see) or ('Series A' in s_see_A) or ('Series B' in s_see_B) :
            country_preseed = ['https://signal.nfx.com'+v for v in response.css('div#stage-pre_seed ul li a ::attr(href)').extract()]
            for country_preseed_link in country_preseed:
                yield scrapy.Request(url=country_preseed_link,callback=self.preseed_func)
            country_seed = ['https://signal.nfx.com'+v for v in response.css('div#stage-seed ul li a ::attr(href)').extract()]
            for country_seed_link in country_seed:
                yield scrapy.Request(url=country_seed_link,callback=self.preseed_func)

            country_seriesA = ['https://signal.nfx.com' + v for v in response.css('div#stage-series_a ul li a ::attr(href)').extract()]
            for country_seriesA_link in country_seriesA:
                yield scrapy.Request(url=country_seriesA_link,callback=self.preseed_func)
            country_seriesB = ['https://signal.nfx.com' + v for v in response.css('div#stage-series_b ul li a ::attr(href)').extract()]
            for country_seriesB_link in country_seriesB:
                yield scrapy.Request(url=country_seriesB_link,callback=self.preseed_func)
        else:
            scripst_tags = response.css('script ::text').extract()
            json_resp = json.loads(
                [v for v in scripst_tags if 'window.__APOLLO_STATE__' in v][0].split('window.__APOLLO_STATE__ = ')[-1])
            type_seed = json_resp[list(json_resp.keys())[0]]['slug']
            investor_count = json_resp[list(json_resp.keys())[0]]['investor_count']
            end_cursor = \
            json_resp[f'${list(json_resp.keys())[0]}' + '.scored_investors({"after":null,"first":8}).pageInfo'][
                'endCursor']
            investors_urls = ['https://signal.nfx.com' + v for v in
                              response.css('a.vc-search-card-name ::attr(href)').extract()]

            if  investor_count > 5:

                while len(investors_urls) < investor_count:
                    payload = {"operationName": "vclInvestors",
                               "variables": {"slug": f"{type_seed}", "order": [{}], "after": f"{end_cursor}"},
                               "query": "query vclInvestors($slug: String!, $after: String) {\n  list(slug: $slug) {\n    id\n    slug\n    investor_count\n    vertical {\n      id\n      display_name\n      kind\n      __typename\n    }\n    location {\n      id\n      display_name\n      __typename\n    }\n    stage\n    firms {\n      id\n      name\n      slug\n      __typename\n    }\n    scored_investors(first: 8, after: $after) {\n      pageInfo {\n        hasNextPage\n        hasPreviousPage\n        endCursor\n        __typename\n      }\n      record_count\n      edges {\n        node {\n          ...investorListInvestorProfileFields\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment investorListInvestorProfileFields on InvestorProfile {\n  id\n  person {\n    id\n    first_name\n    last_name\n    name\n    slug\n    is_me\n    is_on_target_list\n    __typename\n  }\n  image_urls\n  position\n  min_investment\n  max_investment\n  target_investment\n  is_preferred_coinvestor\n  firm {\n    id\n    name\n    slug\n    __typename\n  }\n  investment_locations {\n    id\n    display_name\n    location_investor_list {\n      id\n      slug\n      __typename\n    }\n    __typename\n  }\n  investor_lists {\n    id\n    stage_name\n    slug\n    vertical {\n      id\n      display_name\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n"}
                    headers = {
                        "authority": "signal-api.nfx.com",
                        "accept": "*/*",
                        "accept-language": "en-US,en;q=0.9,de;q=0.8",
                        "content-type": "application/json",
                        "origin": "https://signal.nfx.com",
                        "referer": "https://signal.nfx.com/",
                        "sec-ch-ua": "\"Google Chrome\";v=\"105\", \"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"105\"",
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors",
                        "sec-fetch-site": "same-site",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
                    }

                    graphql_response = json.loads(
                        requests.post(url='https://signal-api.nfx.com/graphql', data=json.dumps(payload),
                                      headers=headers).text)

                    end_cursor = graphql_response['data']['list']['scored_investors']['pageInfo']['endCursor']

                    for api_loop in graphql_response['data']['list']['scored_investors']['edges']:
                        investors_urls.append('https://signal.nfx.com/investors/' + api_loop['node']['person']['slug'])

                for investors_ur in list(set(investors_urls)):
                    if investors_ur not in list(set(previous_urls)):
                        file.write(investors_ur+'\n')
                        file.flush()


        a=2

    def preseed_func(self,response):

        if response.css('main h4 ::text').get() == "This list hasn't launched yet because there are less than 5 investors.":
            pass
        else:
            scripst_tags1 = response.css('script ::text').extract()
            json_resp1 = json.loads([v for v in scripst_tags1 if 'window.__APOLLO_STATE__' in v][0].split('window.__APOLLO_STATE__ = ')[-1])
            type_seed1 = json_resp1[list(json_resp1.keys())[0]]['slug']
            investor_count1 = json_resp1[list(json_resp1.keys())[0]]['investor_count']
            end_cursor1 =  json_resp1[f'${list(json_resp1.keys())[0]}'+'.scored_investors({"after":null,"first":8}).pageInfo']['endCursor']
            investors_urls1 = ['https://signal.nfx.com'+v for v in response.css('a.vc-search-card-name ::attr(href)').extract()]


            if investor_count1 > 5:

                while len(investors_urls1) < investor_count1:


                    payload1 = {"operationName": "vclInvestors",
                               "variables": {"slug": f"{type_seed1}", "order": [{}], "after": f"{end_cursor1}"},
                               "query": "query vclInvestors($slug: String!, $after: String) {\n  list(slug: $slug) {\n    id\n    slug\n    investor_count\n    vertical {\n      id\n      display_name\n      kind\n      __typename\n    }\n    location {\n      id\n      display_name\n      __typename\n    }\n    stage\n    firms {\n      id\n      name\n      slug\n      __typename\n    }\n    scored_investors(first: 8, after: $after) {\n      pageInfo {\n        hasNextPage\n        hasPreviousPage\n        endCursor\n        __typename\n      }\n      record_count\n      edges {\n        node {\n          ...investorListInvestorProfileFields\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment investorListInvestorProfileFields on InvestorProfile {\n  id\n  person {\n    id\n    first_name\n    last_name\n    name\n    slug\n    is_me\n    is_on_target_list\n    __typename\n  }\n  image_urls\n  position\n  min_investment\n  max_investment\n  target_investment\n  is_preferred_coinvestor\n  firm {\n    id\n    name\n    slug\n    __typename\n  }\n  investment_locations {\n    id\n    display_name\n    location_investor_list {\n      id\n      slug\n      __typename\n    }\n    __typename\n  }\n  investor_lists {\n    id\n    stage_name\n    slug\n    vertical {\n      id\n      display_name\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n"}
                    headers1 = {
                        "authority": "signal-api.nfx.com",
                        "accept": "*/*",
                        "accept-language": "en-US,en;q=0.9,de;q=0.8",
                        "content-type": "application/json",
                        "origin": "https://signal.nfx.com",
                        "referer": "https://signal.nfx.com/",
                        "sec-ch-ua": "\"Google Chrome\";v=\"105\", \"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"105\"",
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors",
                        "sec-fetch-site": "same-site",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
                    }


                    graphql_response1 = json.loads(requests.post(url='https://signal-api.nfx.com/graphql', data=json.dumps(payload1), headers=headers1).text)

                    end_cursor1 = graphql_response1['data']['list']['scored_investors']['pageInfo']['endCursor']

                    for api_loop1 in graphql_response1['data']['list']['scored_investors']['edges']:
                        investors_urls1.append('https://signal.nfx.com/investors/'+api_loop1['node']['person']['slug'])

                for investors_ur1  in list(set(investors_urls1)):
                    if investors_ur1 not in list(set(previous_urls)):
                        file.write(investors_ur1 + '\n')
                        file.flush()



process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(investor)
process.start()
