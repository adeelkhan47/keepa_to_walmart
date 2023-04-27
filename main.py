import datetime
import json
import logging
import time
import urllib
from urllib.parse import urlencode

import keepa
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame
from pandas import read_csv
logging.basicConfig(level=logging.INFO)
AccessKey = '7aqr4ii5rnvpaqu7ufm807slm66d1kpvf1btdll8oefeihh6k05bpbnjj2ffb8pn'
SCRAPEOPS_API_KEY = 'aeb417fe-4e6e-45ba-93c4-29119d05ac14'


def scrapeops_url(url):
    payload = {'api_key': SCRAPEOPS_API_KEY, 'url': url, 'country': 'us'}
    proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
    return proxy_url


def create_walmart_product_url(product):
    return 'https://www.walmart.com' + product.get('canonicalUrl', '').split('?')[0]


def fetch_products_from_keepa(asins):
    try:
        api = keepa.Keepa(AccessKey)
        products = api.query(asins)
        return products
    except Exception as e:
        logging.exception(e)
        return []


def fetch_walmart_info(keyword, retry=5):
    for i in range(1, retry + 1):
        try:
            walmart_search_url = f'https://www.walmart.com/search?q={urllib.parse.quote_plus(keyword)}&facet=retailer_type%3AWalmart%7C%7Cexclude_oos%3AShow+available+items+only'
            response = requests.get(scrapeops_url(walmart_search_url))
            if response.status_code == 200:
                html_response = response.text
                soup_html = BeautifulSoup(html_response, "html.parser")
                script_tag = soup_html.find("script", {"id": "__NEXT_DATA__"})
                if script_tag is not None:
                    soup = BeautifulSoup(html_response, 'lxml')
                    price = None
                    name = None
                    prices = soup.find_all('div', attrs={'data-automation-id': 'product-price', 'class': "mb1"})
                    if len(prices) > 0:
                        temp = prices[0]
                        price = temp.find_next().text
                        price = price.replace("Now ", "")
                    names = soup.find_all('span', attrs={'data-automation-id': 'product-title'})
                    if len(names) > 0:
                        name = names[0].text
                    json_blob = json.loads(script_tag.get_text())
                    product_list = json_blob["props"]["pageProps"]["initialData"]["searchResult"]["itemStacks"][0][
                        "items"]
                    product_urls = [create_walmart_product_url(product) for product in product_list]
                    url = product_urls[0]
                    if url and name and price:
                        logging.info(f"Successfully Fetched : {(name, price, url)}")
                        return name, price, url
                    else:
                        return None
                else:
                    logging.error(f"Retry {i}:Failed with 200 Code")
            else:
                logging.error(f"Retry {i}: Failed with 500 Code")
        except Exception as e:
            logging.info(f"Retry {i}: Failed in Exception")
            logging.exception(e)
    return None


def read_input_data():
    data = read_csv("input.csv")
    asins = data['ASINS'].tolist()
    return asins


def right_output(data_fame):
    df = DataFrame(data_fame)
    df.to_csv(f"results/Result{str(time.time()).replace('.', '')}.csv")


def prepare_dataframe(products):
    results = {"asin": [],
               "rootCategory": [],
               "upcList": [],
               "eanList": [],
               "title": [],
               "packageWeight": [],
               "packageQuantity": [],
               "walmart_title": [],
               "walmart_price": [],
               "walmart_url": [],
               "isAdultProduct": [],
               "fbaFees": [],
               "salesRankReference": [],
               "newPriceIsMAP": [],
               # "salesRanks": [],
               # "salesRankReferenceHistory": [],
               "buyBoxSellerIdHistory": [],
               }
    for each in products:
        for key in each.keys():
            if key == "title":
                results[key].append(str(each[key]))
                walmart_data = fetch_walmart_info(each[key])
                if walmart_data:
                    results["walmart_title"].append(walmart_data[0])
                    results["walmart_price"].append(walmart_data[1])
                    results["walmart_url"].append(walmart_data[2])
                else:
                    results["walmart_title"].append("- NA -")
                    results["walmart_price"].append("- NA -")
                    results["walmart_url"].append("- NA -")
            elif key in results.keys():
                if each[key]:
                    results[key].append(str(each[key]))
                else:
                    results[key].append("- NA -")
    return results


logging.info("Fetching ASINS from Input")
asins = read_input_data()
logging.info("Fetching Products from Keepa")
products = fetch_products_from_keepa(asins)
logging.info("Fetching Products Information from Walmart")
logging.info("Preparing Output Sheet.")
data_fame = prepare_dataframe(products)
right_output(data_fame)
logging.info("Success.")
