import asyncio
import pandas as pd
import logging
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, BrowserContext, Playwright, Page
from playwright_stealth import stealth_async
from urllib.parse import urljoin
import re
import json
from datetime import datetime
from random import choice
from amazoncaptcha import AmazonCaptcha
logging.getLogger().setLevel(logging.DEBUG)

async def run_browser() ->  tuple[BrowserContext, Playwright]: 
    p = await async_playwright().start()
    b = choice(['chromium','mozilla'])
    context = {'java_script_enabled' : True, 
               'locale' : 'pt-br',
               'no_viewport' : True}#'viewport' : { 'width': 0, 'height': 0 }}
    if b == 'chromium':
        browser = await p.chromium.launch(headless=False)
    else :
        browser = await p.firefox.launch(headless=False)
    context = await browser.new_context(**context)
    #page = await browser.new_page()
    #        await stealth_async(page)
    return context, p

async def close_browser(p : Playwright) -> None : 
    await p.stop()
    
async def open_pages(context : BrowserContext) -> Page:
    page = await context.new_page()
    return page

async def scrap(page : Page, item : str, itemId : int) -> list:
    scrap = await scrap_amazon(page, item, itemId)
    return scrap

async def scrap_amazon(page : Page, item : str, itemId : int) -> list:
    base_url='https://www.amazon.com.br/'
    search_url = urljoin(base_url, f'/s?k={item}&page=1')
    await page.goto(search_url)
    while await page.get_by_alt_text('Desculpe! Algo deu errado').count() > 0: await page.goto(base_url)
    if await page.get_by_role("button", name="Continuar comprando").count() > 0: 
        await captcha(page)
        await page.goto(search_url)
    html = await page.content()
    soup = BeautifulSoup(html, 'html.parser')
    item_details = await amazon_data_parse(soup, item, itemId)
    item_details = await get_ean(page, item_details)
    return item_details

async def captcha(page : Page) -> None:
    link = await page.locator("div.a-row:nth-child(2) > img:nth-child(1)").get_attribute('src')
    captcha = AmazonCaptcha.fromlink(link)  
    solution = captcha.solve()
    await page.get_by_placeholder("Digitar caracteres").fill(solution)
    await page.get_by_role("button", name="Continuar comprando").click()
    

async def amazon_data_parse(soup : BeautifulSoup, item_name : str, itemId : int) -> list:
    itens = soup.find_all('div',{'data-component-type' : "s-search-result"})
    item_list = []
    for count, item in enumerate(itens) : 
        
        def get_element(item : BeautifulSoup, selector : tuple[str,dict], attribute : str | None = None, text : bool = False, failsafe_value : str | int | None = None) -> str | int | None:
            try:
                element = item.find(selector[0],selector[1])
                element = element.get(attribute) if attribute else element # type: ignore
                element = element.get_text() if text else element # type: ignore
                return element if element else failsafe_value # type: ignore
            except (AttributeError, IndexError):
                return failsafe_value

        title = get_element(item,selector=('span',{'class' : 'a-size-base-plus a-color-base a-text-normal'}), text=True)
        
        asin = item.get('data-asin')
    
        label = get_element(item, selector=('span',{'aria-label' : 'Escolha da Amazon'}), attribute='aria-label',failsafe_value=None)
    
        current_whole_price = get_element(item, selector=('span',{'class' : 'a-price-whole'}), text=True, failsafe_value='0').replace(',','').replace('.','') # type: ignore      
        current_fraction_price = get_element(item, selector=('span',{'class' : 'a-price-fraction'}),text=True ,failsafe_value='0')
        current_price = float(current_whole_price+'.'+current_fraction_price) # type: ignore
    
        try:
            rating_info = get_element(item, selector=('div',{'class':'a-row a-size-small'}),text=True, failsafe_value=None)
            rating = float(rating_info[:3].replace(',','.')) # type: ignore
            rating_count = int(re.findall(r'\d+', rating_info)[-1]) # type: ignore
        #     rating_info = item.find('div', {'class':'a-row a-size-small'}).get_text()
        #     rating = float(rating_info[:3].replace(',','.'))
        except : 
            rating =  None
            rating_count = None


        ad = True if (get_element(item, selector=('span',{'class' : 'a-color-secondary'}),text=True, failsafe_value=None) == 'Patrocinado') else False
        # try:
        #     ad = True if (item.find_all('span', {'class' : 'a-color-secondary'})[0].get_text() == 'Patrocinado') else False
        # except IndexError:
        #     ad = False
             
        _ = {'productId' : itemId,
            'asin' : asin,
            'opt_label' : label,
            "ad": ad,
            'title' : title,
            'current_price' : current_price,
            'url':f'https://www.amazon.com.br/dp/{asin}',
            'rating' : rating,
            'rating_count' : rating_count,
            'ean': None,
            'similarity_ratio' : simulatity_ratio(item_name, str(title))
            }
        item_list.append(_)
    return item_list

async def get_ean(page : Page, item_list : list[dict]):
        item_list_ean = []
        for item in item_list:
            url = item['url']
            await page.goto(url)
            if page.get_by_role("cell", name="EAN") is not None:
                locator = page.locator ('css = #productDetails_techSpec_section_1').locator('.prodDetAttrValue').last
                item['ean'] = await locator.inner_text()
                try:
                    item['ean'] = item['ean'].replace('\u200e','')
                except:
                    pass
            print(item)

            item_list_ean.append(item)
        return item_list_ean


async def save_results(resultados : list, title : str):
    safe_title = "".join(c for c in title if c.isalpha() or c.isdigit() or c==' ').rstrip()
    with open(f'Data extraction\\{safe_title} - {datetime.now().strftime("%Y%m%d-%H%M")}.json', 'w',) as fp:
        newlist = sorted(resultados, key=lambda d: d['similarity_ratio'], reverse=True) 
        json.dump(newlist, fp)

def simulatity_ratio (original_string : str, found_string : str) -> float:
    s = SequenceMatcher(None, str.lower(original_string), str.lower(found_string))
    return s.ratio()

async def run_scrap(item_list : pd.DataFrame) -> None:
    context, p = await run_browser()
    page = await open_pages(context)
    for item in product_item_list.iterrows():
        item_name = item[1]['productName']
        itemId = item[1]['productId']
        html = await scrap(page, item_name, itemId)
        await save_results(html, item_name)
    await close_browser(p)
    

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

asyncio.run(run_scrap(product_item_list))