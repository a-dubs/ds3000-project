import requests
from bs4 import BeautifulSoup
from alive_progress import alive_bar, alive_it
import os
from crawler import *

# to scrape 
# team's stats for maps - rip data week by week: (https://www.hltv.org/stats/teams/maps/5995/g2?startDate=2022-02-06&endDate=2022-03-06)
# top 30 rankings each day / week (https://www.hltv.org/ranking/teams/2022/january/31)
# only scrape matches after June 6, 2017
# maybe event track records for each team? (https://www.hltv.org/stats/teams/events/5995/g2?startDate=all)
# to get only top 30: https://www.hltv.org/stats/matches?startDate=all&offset=13000&rankingFilter=Top30
os.chdir(os.path.dirname(os.path.realpath(__file__)))
hltv_base_url = "https://www.hltv.org"

def is_valid_match_page(html : str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    return len(soup.select("table.stats-table")) == 2

def get_teams_stats_table(html : str):
    soup = BeautifulSoup(html, "html.parser")
    return soup.select("table.stats-table")[0], soup.select("table.stats-table")[1]
    
# returns a list of 5 players stats which are flattened lists of (key, value) pairings (tuples)
def get_player_stats(table):
    result = {}
    players = []

    try:
        result["team_name"] = table.select("thead")[0].select("th.st-teamname")[0].text
        columns = table.select("thead")[0].select("th")[1:]
        rows = table.select("tbody")[0].select("tr")
        for row in rows:
            pstats = []
            stats = row.select("td")
            for stat in stats:
                stat_name = stat["class"][0][3:]
                stat_value = (stat.text.split(" ")[0]).strip()
                pstats.append((stat_name, stat_value))

                if stat_name == "kills":
                    # get secondary stat in parenthesis and then trim off the parenthesis to keep just the value
                    pstats.append(("hskills", stat.text.split()[1][1:-1].strip() if "(" in stat.text else ""))
                if stat_name == "assists":
                    # get secondary stat in parenthesis and then trim off the parenthesis to keep just the value
                    pstats.append(("fassists", stat.text.split()[1][1:-1].strip() if "(" in stat.text else ""))
            players.append(pstats)
    except:
        print(table)
    return players

num_match_stats_loaded = 0
completed_matches_urls = []

def load_matches_stats_csv():
    global completed_matches_urls, num_match_stats_loaded
    # load previously stored matches urls
    with open("matches_stats.csv", "r") as f:
        completed_matches_urls = [l.split(",")[0] for l in f.readlines()[1:]]
    num_match_stats_loaded = len(completed_matches_urls)

def extract_player_stats(match_url : str, match_date : str):
    # if only given a uri, turn into full url
    if not "http" in match_url:
        match_url = hltv_base_url + match_url
    r = None
    while True:
        r = requests.get(match_url)
        if is_valid_match_page(r.text):
            break
        print(r.text)
        input()
        time.sleep(10)

    tables = get_teams_stats_table(r.text)
    team1_player_stats, team2_player_stats = get_player_stats(tables[0]) , get_player_stats(tables[1])

    new_team_stats = []
    for pn in range(1,len(team1_player_stats)+1):
        pstats = team1_player_stats[pn-1]
        for tuple in pstats:
            new_team_stats.append((f"t1p{pn}_"+tuple[0], tuple[1]))
    for pn in range(1,len(team2_player_stats)+1):
        pstats = team2_player_stats[pn-1]
        for tuple in pstats:
            new_team_stats.append((f"t2p{pn}_"+tuple[0], tuple[1]))

    # print("".join([f"\n{tuple[0]} : {tuple[1]}, " if "player" in tuple[0] else f"{tuple[0]} : {tuple[1]}, " 
    #     for tuple in new_team_stats]))
    try:
            
        with open("matches_stats.csv", "a", encoding='utf-8') as f:
            f.write(match_url + ", " + match_date + ", ")
            f.write(", ".join([item[1] for item in new_team_stats]) + "\n")
    except:
        print(r.text)
        print(new_team_stats)

def scrape_matches_pages():

    num_matches_stats_to_scrape = input("How many matches stats do you want to scrape? ")

    if num_matches_stats_to_scrape and (num_matches_stats_to_scrape:=int(num_matches_stats_to_scrape)) > 0:
        load_matches_stats_csv()
        num_matches_stats_scraped = 0
        urls = set(matches_urls.keys())
        with alive_bar(num_matches_stats_to_scrape, bar="filling", length=60, title="Scraping Matches Pages") as bar:
            for url in urls:
                if num_matches_stats_scraped >= num_matches_stats_to_scrape:
                    break
                if not url in completed_matches_urls: 
                    extract_player_stats(url, matches_urls[url])
                    completed_matches_urls.append(url)
                    num_matches_stats_scraped += 1
                    bar() 

        print(f"number of new/unique matches stats scraped: {num_match_stats_loaded} -> {len(completed_matches_urls)}")


scrape_matches_pages()