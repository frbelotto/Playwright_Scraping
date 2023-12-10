import asyncio
import logging
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from urllib.parse import urljoin
import re
import json
logging.getLogger().setLevel(logging.DEBUG)

async def run_browser() : 
    p = await async_playwright().start()
    browser = await p.chromium.launch(headless=False)
    context = await browser.new_context(java_script_enabled=True,locale='pt-br')
    page = await context.new_page()
    return page, p

async def close_browser(p) : 
    await p.stop()

async def scrap(page, item) :
    scrap = await scrap_amazon(page, item)
    return scrap

async def scrap_amazon(page, item):
    base_url='https://www.amazon.com.br/'
    search_url = urljoin(base_url, f'/s?k={item}&page=1')
    await page.goto(search_url)
    html = await page.content()
    soup = BeautifulSoup(html, 'html.parser')
    item_details = await amazon_data_parse(soup, item)
    return item_details

def errhandling (expression, failsafe_value):
    try :
        value = expression
    except:
        value = failsafe_value
    return value

async def amazon_data_parse(soup, item_name):
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
        
        _ = {'index': count,
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
    with open(f'Data extraction\\data_{title}.json', 'w',) as fp:
        newlist = sorted(resultados, key=lambda d: d['similarity_ratio'], reverse=True) 
        json.dump(newlist, fp)

def simulatity_ratio (original_string : str, found_string : str) -> float:
    s = SequenceMatcher(None, str.lower(original_string), str.lower(found_string))
    return s.ratio()


async def run_scrap(item):
    page,p = await run_browser()
    html = await scrap(page, item)
    await save_results(html, item)
    #sleep(30)
    await close_browser(p)
    return html
    

keyword_list = ['whisky jhonnie walker blue label - 750ml', 'whisky old parr 18 anos - 750ml', "whisky ballantine´s 12 anos 1L"]
#resultado =  asyncio.run(run_scrap(keyword_list[1]))
#print(f'foram capturados {len(resultado)} resultados')
#print (resultado)
for word in keyword_list:
    asyncio.run(run_scrap(word))




# async def main(keyword : str):    
#     soup = await web_browser_search(keyword)
    

# async def web_browser_search(keyword) :
#     async with async_playwright() as p:
#         #carrega navegador e extrai conteúdo da página
#         browser = await p.chromium.launch(headless=True)
#         context = await browser.new_context(java_script_enabled=False,locale='pt-br', base_url='https://www.amazon.com.br/')
#         page = await context.new_page()
#         await page.goto(f'/s?k={keyword}&page=1')
#         soup = BeautifulSoup(await page.content(), features='html.parser' )
#         #sleep(0.5)
#         #page.on("response", lambda response: print("<<", response.status, response.url, response.body))
#         # print(soup)
#         # print('================')
#         # print(await page.content())
#         #page.on("response", lambda response: print(response.body))
#             #response = await response.body();

#         resultados  = parse_search_results(keyword, soup)
#         print(resultados)
#         await browser.close()

# def parse_search_results(keyword : str, content):
#     resultados = []
#     search_products = content.select("div.s-result-item[data-component-type=s-search-result]")
#     for product in search_products:
#         relative_url  = product.select("h2")[0].findChild("a")['href']
#         product_url = urljoin('https://www.amazon.com.br/', relative_url).split("?")[0]
#         asin = relative_url.split('/')[3] if len(relative_url.split('/')) >= 4 else None
#         return {'list' : content.find_all("span", attrs={'data-component-type="s-search-results"'}),
#             "keyword": keyword,
#             "asin": asin,
#             "url": product_url,
#             "ad": "/slredirect/" in product_url,
#             'title' : product.select("h2 > a > span")[0].text,
#              }
        
        
        





# # def parse_search_results(keyword : str, content : page.c, page ):
# #     resultados = []
# #     search_products = content.select("div.s-result-item[data-component-type=s-search-result]")
# #     for product in search_products[1]:
# #         for link in product.find("h2", class_="a"):
# #             relative_url  = link.get('href')
# #             product_url = urljoin('https://www.amazon.com.br/', relative_url).split("?")[0]
# #             asin = relative_url.split('/')[3] if len(relative_url.split('/')) >= 4 else None
#     #     page.goto(product_url)
#     #     return {
#     #             "keyword": keyword,
#     #             "asin": asin,
#     #             "url": product_url,
#     #             "ad": "/slredirect/" in product_url,
#     #             "title": BeautifulSoup(page.inner_html("h2 > a > span"), "lxml").text,
#     #             "price": page.inner_text(".a-price[data-a-size=xl] .a-offscreen"),
#     #             "real_price": page.inner_text(".a-price[data-a-size=b] .a-offscreen"),
#     #             "rating": (page.inner_text("span[aria-label~=stars]").re(r"(\d+\.*\d*) out") or [None])[0],
#     #             "rating_count": page.inner_text("span[aria-label~=stars] + span[aria-label]"),
#     #             "thumbnail_url": page.query_selector("//img.s-image")["src"],
#     #         }
#             #resultados.append((product_url, asin))
#         #return product, a



# # def parse_search_results(content : bs4.BeautifulSoup):
# #     # Extract Overview Product Data
# #     search_products = content.css("div.s-result-item[data-component-type=s-search-result]")
# #     for product in search_products:
# #         print(product)
# #             #relative_url = product.css("h2>a::attr(href)").get()
# #             #asin = relative_url.split('/')[3] if len(relative_url.split('/')) >= 4 else None
# #         #     product_url = urljoin('https://www.amazon.com.br/', relative_url).split("?")[0]
# #         #     yield  {
# #         #             "keyword": keyword,
# #         #             "asin": asin,
# #         #             "url": product_url,
# #         #             "ad": True if "/slredirect/" in product_url else False, 
# #         #             "title": BeautifulSoup(product.css("h2>a>span::text").get(), "lxml").text,
# #         #             #"title": product.css("h2>a>span::text").get(),
# #         #             "price": product.css(".a-price[data-a-size=xl] .a-offscreen::text").get(),
# #         #             "real_price": product.css(".a-price[data-a-size=b] .a-offscreen::text").get(),
# #         #             "rating": (product.css("span[aria-label~=stars]::attr(aria-label)").re(r"(\d+\.*\d*) out") or [None])[0],
# #         #             "rating_count": product.css("span[aria-label~=stars] + span::attr(aria-label)").get(),
# #         #             "thumbnail_url": product.xpath("//img[has-class('s-image')]/@src").get(),
# #         #         }


# for item in keyword_list:
#     asyncio.run(main(item))