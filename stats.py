import requests
from pathlib import Path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import json


def get_rate(url):
    try:
        html = requests.get(url, stream=True)
        data = html.json()
        return data
    except requests.exceptions.RequestException as e:
        return "-1"


def runner(url_list):
    with ThreadPoolExecutor(max_workers=64) as executor:
        future_list = []
        for url in url_list:
            future = executor.submit(get_rate, url)
            future_list.append(future)
        results = []
        for f in as_completed(future_list):
            results.append(f.result())
        product_ls = [i.split("/")[-1] for i in url_list]
    return {k:v for k,v in zip(product_ls,results)}


path = Path("Product_IDs")
files = [x for x in path.glob('*') if x.is_file()]

results = {}
for f in files:
    name = f.stem.split('-')[0]
    print(f"Get rate of {name}")
    df = pd.read_csv(f, header=None)
    url_list = []
    for product in df[0]:
        url_list.append(f'https://reviewmeta.com/api/amazon/{product}')
    with open(f"results/{f.stem}.json", "w") as outfile:
        json.dump(runner(url_list), outfile)
    print(f"Done with {f.stem}")
