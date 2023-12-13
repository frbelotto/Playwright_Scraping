import asyncio
import logging
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from urllib.parse import urljoin
import re
import json
from datetime import datetime
logging.getLogger().setLevel(logging.DEBUG)

async def run_browser() : 
    p = await async_playwright().start()
    browser = await p.chromium.launch(headless=False)
    context = await browser.new_context(java_script_enabled=True,locale='pt-br')
    page = await context.new_page()
    return page, p

async def close_browser(p) : 
    await p.stop()

async def scrap(page, item, itemId) :
    scrap = await scrap_amazon(page, item, itemId)
    return scrap

async def scrap_amazon(page, item, itemId):
    base_url='https://www.amazon.com.br/'
    search_url = urljoin(base_url, f'/s?k={item}&page=1')
    await page.goto(search_url)
    html = await page.content()
    soup = BeautifulSoup(html, 'html.parser')
    item_details = await amazon_data_parse(soup, item, itemId)
    return item_details

async def amazon_data_parse(soup, item_name, itemId):
    itens = soup.find_all('div',{'data-component-type' : "s-search-result"})
    item_list = []
    for count, item in enumerate(itens) : 
        asin = item.get('data-asin')
        title = item.find_all('span',{'class' : 'a-size-base-plus a-color-base a-text-normal'})[0].get_text()

        try: 
            label = item.find_all('span',{'aria-label' : 'Escolha da Amazon'})[0].get('aria-label')
        except IndexError :
            label = None
            
        try:
            current_whole_price = item.find_all('span', {'class' : 'a-price'})[0].find('span', {'class' : 'a-price-whole'}).get_text().replace(',','').replace('.','')
        except:
            current_whole_price = '0'
        
        try : 
            current_fraction_price = item.find_all('span', {'class' : 'a-price'})[0].find('span', {'class' : 'a-price-fraction'}).get_text() 
        except : 
            current_fraction_price = '0'
        current_price = float(current_whole_price+'.'+current_fraction_price)

        try : 
            rating_info = item.find('div', {'class':'a-row a-size-small'}).get_text()
            rating = float(rating_info[:3].replace(',','.'))
            rating_count = int(re.findall(r'\d+', rating_info)[-1])
        except : 
            rating =  None
            rating_count = None

        try:
            ad = True if (item.find_all('span', {'class' : 'a-color-secondary'})[0].get_text() == 'Patrocinado') else False
        except IndexError:
            ad = False
        
        _ = {'productId' : itemId,
            'asin' : asin,
            'opt_label' : label,
            #"ad": True if (item.find_all('span', {'class' : 'a-color-secondary'})[0].get_text() == 'Patrocinado') else False ,
            "ad": ad,
            'title' : title,
            'current_price' : current_price,
            'url':f'https://www.amazon.com.br/dp/{asin}',
            'rating' : rating,
            'rating_count' : rating_count,
            'similarity_ratio' : simulatity_ratio(item_name, title)
            }
        item_list.append(_)
    return item_list#)item_list#(itens#, 

async def save_results(resultados, title):
    with open(f'Data extraction\\{title} - {datetime.now().strftime("%Y%m%d-%H%M")}.json', 'w',) as fp:
        newlist = sorted(resultados, key=lambda d: d['similarity_ratio'], reverse=True) 
        json.dump(newlist, fp)

def simulatity_ratio (original_string : str, found_string : str) -> float:
    s = SequenceMatcher(None, str.lower(original_string), str.lower(found_string))
    return s.ratio()


async def run_scrap(item_list):
    page,p = await run_browser()
    for item in product_item_list.iterrows():
        item_name = item[1]['productName']
        itemId = item[1]['productId']
        html = await scrap(page, item_name, itemId)
        await save_results(html, item_name)
    # for item in item_list:
    #     html = await scrap(page, item_name)
    #     await save_results(html, item_name)
    await close_browser(p)
    

import pandas as pd
input = pd.read_json(r'C:\Users\fabio\OneDrive\Python\Playwright_Scraping\Data input\result.json')
df = pd.json_normalize(input['data']['productSearch']['products'])
item_list = []
for row in df.iterrows():
    item_detail = {
        'productId' : row[1]['productId'],
        'productName' : row[1]['productName'],
        'itemId' : row[1]['items'][0]['itemId'],
        'ean' : row[1]['items'][0]['ean'],
        'sellername' : row[1]['items'][0]['sellers'][0]['sellerName'],
        'ListPrice' : row[1]['items'][0]['sellers'][0]['commertialOffer']['ListPrice']
    }   
    item_list.append(item_detail)
df = pd.DataFrame(item_list)
product_item_list = df[['productId','productName']]

#keyword_list = ['whisky jhonnie walker blue label - 750ml', 'whisky old parr 18 anos - 750ml', "whisky ballantine´s 12 anos 1L"]
asyncio.run(run_scrap(product_item_list))