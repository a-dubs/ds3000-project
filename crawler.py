import requests
from bs4 import BeautifulSoup
from alive_progress import alive_bar, alive_it
import os
import csv 
import time

os.chdir(os.path.dirname(os.path.realpath(__file__)))

matches_urls = {}
num_matches_loaded = 0

def load_matches_urls_csv():
    global matches_urls, num_matches_loaded
    # load previously stored matches urls
    with open("all_matches_urls.csv", "r") as f:
        matches_urls = {l.split(",")[0].strip():l.split(",")[1].strip() for l in f.readlines()[1:]}
    num_matches_loaded = len(matches_urls)


def format_hltv_date(hltv_date : str):
    day, month, year = hltv_date.split("/")
    return "-".join((f"20{year}", month.rjust(2,'0'), day.rjust(2,'0')))

def save_matches_urls_csv():
    with open("all_matches_urls.csv", "w") as f:
        f.write("match_url,match_date\n")
        for url in matches_urls:
            f.write(f"{url},{matches_urls[url]}\n")
    
def get_matches_urls(matches_page_html_text : str):
    global matches_urls
    match_list = []
    while True:
        soup = BeautifulSoup(matches_page_html_text, "html.parser")
        match_list=soup.select("tr.group-1") + soup.select("tr.group-2")    
        if len(match_list) == 50:
            break
        time.sleep(1)
    # before = len(matches_urls)
    for entry in match_list:
        match_url = entry.select("td.date-col")[0].select("a")[0]["href"]
        match_date = format_hltv_date(entry.select("td.date-col")[0].select("a")[0].select("div.time")[0].text)
        matches_urls[match_url] = match_date
        
def get_matches_page_html(offset : int = 0) -> str:
    # round down to interval of 50
    url=f"https://www.hltv.org/stats/matches?offset={offset}"
    response = requests.get(url)
    return response.text

def crawl(num_matches, starting_offset):
    for query_offset in alive_it(range(0, num_matches, 50)):
        if query_offset >= starting_offset:
            html = get_matches_page_html(offset=query_offset)
            get_matches_urls(html)
            save_matches_urls_csv()
            with open("crawler_last_page_no.txt","w") as f:
                f.write(str(int(query_offset/50)))

load_matches_urls_csv()

# num_match_urls_to_get = input("How many total match urls do you want to fetch? ")
# last_page_no = input("What page number did you get to last? ") 
# last_page_no = 0 if last_page_no == "" else int(last_page_no) 

num_match_urls_to_get = 80000
with open("crawler_last_page_no.txt","r") as f:
    last_page_no = int(f.read())

# last_page_no = 0
num_match_urls_to_get = 0

if num_match_urls_to_get and (num_match_urls_to_get:=int(num_match_urls_to_get)) > 0:
    crawl(num_match_urls_to_get, last_page_no*50)
    print(f"number of matches_urls: {num_matches_loaded} -> {len(matches_urls)}")
    save_matches_urls_csv()
