import requests
from bs4 import BeautifulSoup
from alive_progress import alive_bar, alive_it
import os
import csv
from datetime import datetime, timedelta
import re

# to scrape 
# team's stats for maps - rip data week by week: (https://www.hltv.org/stats/teams/maps/5995/g2?startDate=2022-02-06&endDate=2022-03-06)
# top 30 rankings each day / week (https://www.hltv.org/ranking/teams/2022/january/31)  
# only scrape matches after June 6, 2017
# maybe event track records for each team? (https://www.hltv.org/stats/teams/events/5995/g2?startDate=all)
# to get only top 30: https://www.hltv.org/stats/matches?startDate=all&offset=13000&rankingFilter=Top30
os.chdir(os.path.dirname(os.path.realpath(__file__)))

hltv_base_url = "https://www.hltv.org"

def is_valid_page(html : str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    return len(soup.select("div.regional-ranking-header")) == 1 

scraped_urls = []
scraped_dates = []
team_rankings_each_day = []
num_urls_loaded = 0
nums_days_loaded = 0

team_rankings_csv_fields = ["date","url"]
for n in range(1,31):
    team_rankings_csv_fields += [f"{n}_team_name", f"{n}_team_id", f"{n}_team_ranking_points"]

# print(team_rankings_csv_fields)

def load_csv():
    global scraped_urls, scraped_dates, num_urls_loaded, nums_days_loaded
    
    # with open("team_rankings_urls.csv", "r", encoding="utf-8") as f:
        # scraped_urls = [l.strip() for l in f.readlines()]
    with open("team_rankings.csv", "r", encoding="utf-8") as f:
        if len(list(f.readlines())) <= 2:
            print("EMPTY CSV FUCKK")
            return
    with open("team_rankings.csv", "r", encoding="utf-8") as f:
        csvreader = csv.reader(f, delimiter=',') 
        header_skipped = False
        for row in csvreader: 
            if not header_skipped:
                row = next(csvreader)
                header_skipped = True
            if len(row) == len(team_rankings_csv_fields):
                team_rankings_each_day.append(row)
                if row[1] not in scraped_urls:
                    scraped_urls.append(row[1])

    nums_days_loaded = len(team_rankings_each_day)
    num_urls_loaded = len(scraped_urls)

def save_csv():
    with open("team_rankings.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(team_rankings_csv_fields)
        writer.writerows(team_rankings_each_day)

def parse_date_from_url(url : str) -> datetime:
    url_datetime_info = url.split("/")[-3:]
    dt_obj = datetime.strptime(" ".join(url_datetime_info), "%Y %B %d")
    return dt_obj

# stop datetime object is exclusive i.e. will not be included : [start_dt, stop_dt)
def get_datetimes_between_dates(start_dt : datetime, stop_dt : datetime):
    result = []
    if start_dt > stop_dt:
        temp = start_dt
        start_dt = stop_dt
        stop_dt = temp
    while start_dt < stop_dt:
        result.append(start_dt)
        start_dt = start_dt + timedelta(days=1)
    return result

def get_next_week(current_dt: datetime):
    days_off = 7 if (abs((current_dt - datetime(2022,3,7)).days) % 7) == 0 else (abs((current_dt - datetime(2022,3,7)).days) % 7)
    new_dt = current_dt - timedelta(days=days_off)
    week_of_dts = get_datetimes_between_dates(current_dt, new_dt)
    week_url = f"https://www.hltv.org/ranking/teams/{new_dt.strftime('%Y')}/{new_dt.strftime('%B')}/{new_dt.strftime('%d')}".lower()
    return new_dt, week_of_dts, week_url

def get_rank_boxes(html : str):
    soup = BeautifulSoup(html, "html.parser")
    return soup.select("div.ranked-team")

def scrape_team_rankings(url : str) -> list[str]:
    r = requests.get(url)

    if not is_valid_page(html:=r.text):
        if "<h1>404</h1>" in html and '<div class="error-desc">Page not found</div>' in html:
            print("date got fucked up somehow...")
            print(url)
            return []
        print("they rate limited my ass D:")
        print(url)
        with open("failed_rankings_page.html", "w", encoding="utf-8") as f:
            f.write(html)
        exit()
    
    boxes = get_rank_boxes(html)
    extracted_info = []
    
    for box in boxes:
        name = box.select("div.lineup-con")[0].select("a.moreLink")[0]["href"].split("/")[3]
        id = box.select("div.lineup-con")[0].select("a.moreLink")[0]["href"].split("/")[2]
        pts = re.sub("[^0-9]", "", box.select("div.teamLine")[0].select("span.points")[0].text)
        extracted_info += [name, id, pts]   
    
    return extracted_info

def scrape_and_crawl_team_rankings():

    weeks_to_scrape = input("How many weeks back of team rankings do you want to scrape? ")
    if weeks_to_scrape and (weeks_to_scrape:=int(weeks_to_scrape)) > 0:
        start_dt = datetime(datetime.now().year, datetime.now().month, datetime.now().day) + timedelta(days=1)
        dt_it = start_dt
        load_csv()
        with alive_bar(weeks_to_scrape, bar="filling", length=30, title="Scraping Matches Pages") as bar:
            for week_n in range(0, weeks_to_scrape):
                dt_it, dt_batch, week_url = get_next_week(dt_it)
                # print(dt_it)
                # print(dt_batch)
                # print(week_url)
                # print("\n\n")
                if week_url not in scraped_urls:
                    scraped_urls.append(week_url)
                    ranking_data = []
                    while True:
                        ranking_data = scrape_team_rankings(week_url)

                        if len(ranking_data) == 0:
                            dt_it, dt_batch, week_url = get_next_week(dt_it + timedelta(days=1))
                        else:
                            break

                    
                    for dt in dt_batch:
                        entry = [dt.strftime("%Y-%m-%d"), week_url] + ranking_data
                        # if existing:=[x for x in team_rankings_each_day if x[0] == entry[0]]:
                        #     for item in existing:
                        #         team_rankings_each_day.remove(item)
                        team_rankings_each_day.append(entry)
                    save_csv()
                bar()

        print(f"Days of rankings scraped: {nums_days_loaded} -> {len(team_rankings_each_day)}")

# print(get_datetimes_between_dates( datetime.now() + timedelta(days=7), datetime.now()))
# print(parse_date_from_url("https://www.hltv.org/ranking/teams/2022/march/7"))
# print(get_next_week(datetime(datetime.now().year, datetime.now().month, datetime.now().day) + timedelta(days=1)))
# save_csv()



scrape_and_crawl_team_rankings()
