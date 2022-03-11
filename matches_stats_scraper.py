from re import S
from urllib import request
import requests
from bs4 import BeautifulSoup
from alive_progress import alive_bar, alive_it
import os
# import time
from crawler import *

# to scrape 
# team's stats for maps - rip data week by week: (https://www.hltv.org/stats/teams/maps/5995/g2?startDate=2022-02-06&endDate=2022-03-06)
# top 30 rankings each day / week (https://www.hltv.org/ranking/teams/2022/january/31)
# only scrape matches after June 6, 2017
# maybe event track records for each team? (https://www.hltv.org/stats/teams/events/5995/g2?startDate=all)
# to get only top 30: https://www.hltv.org/stats/matches?startDate=all&offset=13000&rankingFilter=Top30
os.chdir(os.path.dirname(os.path.realpath(__file__)))
hltv_base_url = "https://www.hltv.org"



match_stats_csv_fields = [
    "match_url", "match_date", "map",

    "t1_name", "t1_id", "t1_total_rw", "t1_fh_rw", "t1_sh_rw", "t1_ot_rw", "t1_rating", "t1_fkills", "t1_cw",

    "t1p1_player", "t1p1_kills", "t1p1_hskills", "t1p1_assists", "t1p1_fassists", "t1p1_deaths", "t1p1_kdratio", 
    "t1p1_kddiff", "t1p1_adr", "t1p1_fkdiff", "t1p1_rating", "t1p2_player", "t1p2_kills", "t1p2_hskills", 
    "t1p2_assists", "t1p2_fassists", "t1p2_deaths", "t1p2_kdratio", "t1p2_kddiff", "t1p2_adr", "t1p2_fkdiff", 
    "t1p2_rating", "t1p3_player", "t1p3_kills", "t1p3_hskills", "t1p3_assists", "t1p3_fassists", "t1p3_deaths", 
    "t1p3_kdratio", "t1p3_kddiff", "t1p3_adr", "t1p3_fkdiff", "t1p3_rating", "t1p4_player", "t1p4_kills", 
    "t1p4_hskills", "t1p4_assists", "t1p4_fassists", "t1p4_deaths", "t1p4_kdratio", "t1p4_kddiff", "t1p4_adr", 
    "t1p4_fkdiff", "t1p4_rating", "t1p5_player", "t1p5_kills", "t1p5_hskills", "t1p5_assists", "t1p5_fassists", 
    "t1p5_deaths", "t1p5_kdratio", "t1p5_kddiff", "t1p5_adr", "t1p5_fkdiff", "t1p5_rating", 

    "t2_name", "t2_id", "t2_total_rw", "t2_fh_rw", "t2_sh_rw", "t2_ot_rw", "t2_rating", "t2_fkills", "t2_cw",
    
    "t2p1_player", "t2p1_kills", "t2p1_hskills", "t2p1_assists", "t2p1_fassists", "t2p1_deaths", "t2p1_kdratio", 
    "t2p1_kddiff", "t2p1_adr", "t2p1_fkdiff", "t2p1_rating", "t2p2_player", "t2p2_kills", "t2p2_hskills", 
    "t2p2_assists", "t2p2_fassists", "t2p2_deaths", "t2p2_kdratio", "t2p2_kddiff", "t2p2_adr", "t2p2_fkdiff", 
    "t2p2_rating", "t2p3_player", "t2p3_kills", "t2p3_hskills", "t2p3_assists", "t2p3_fassists", "t2p3_deaths", 
    "t2p3_kdratio", "t2p3_kddiff", "t2p3_adr", "t2p3_fkdiff", "t2p3_rating", "t2p4_player", "t2p4_kills", 
    "t2p4_hskills", "t2p4_assists", "t2p4_fassists", "t2p4_deaths", "t2p4_kdratio", "t2p4_kddiff", "t2p4_adr", 
    "t2p4_fkdiff", "t2p4_rating", "t2p5_player", "t2p5_kills", "t2p5_hskills", "t2p5_assists", "t2p5_fassists", 
    "t2p5_deaths", "t2p5_kdratio", "t2p5_kddiff", "t2p5_adr", "t2p5_fkdiff", "t2p5_rating"
    ]

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
            pstats = {}
            stats = row.select("td")
            for stat in stats:
                stat_name = stat["class"][0][3:]
                stat_value = (stat.text.split(" ")[0]).strip()
                pstats[stat_name] = stat_value

                if stat_name == "kills":
                    # get secondary stat in parenthesis and then trim off the parenthesis to keep just the value
                    pstats["hskills"] =  stat.text.split()[1][1:-1].strip() if "(" in stat.text else ""
                if stat_name == "assists":
                    # get secondary stat in parenthesis and then trim off the parenthesis to keep just the value
                    pstats["fassists"] = stat.text.split()[1][1:-1].strip() if "(" in stat.text else ""
            players.append(pstats)
    except:
        print(table)
    return players

num_match_stats_loaded = 0
completed_matches_urls = []
match_stats = []

last_scraped_match_stats = {}  # for debug purposes only


def load_matches_stats_csv():
    global completed_matches_urls, num_match_stats_loaded, match_stats
    # load previously stored match stats
    with open("matches_stats.csv", "r", encoding="utf-8") as f:
        if len(list(f.readlines())) <= 2:
            print("EMPTY CSV FUCKK")
            return
    with open("matches_stats.csv", "r", encoding="utf-8") as f:
        csvdictreader = csv.DictReader(f, delimiter=',') 
        for row in csvdictreader: 
            # print(len(row), len(match_stats_csv_fields))
            if len(row) == len(match_stats_csv_fields):
                print("true")
                match_stats.append(row)
                if row["match_url"] not in completed_matches_urls:
                    completed_matches_urls.append(row["match_url"])
    num_match_stats_loaded = len(match_stats)
    save_full_csv()

def save_full_csv():
    # try:
    with open("matches_stats.csv", "w", encoding="utf-8", newline="") as f:
        dictwriter = csv.DictWriter(f, fieldnames=match_stats_csv_fields)
        dictwriter.writeheader()
        dictwriter.writerows(match_stats)

def append_csv():
    # try:
    with open("matches_stats.csv", "a", encoding="utf-8", newline="") as f:
        dictwriter = csv.DictWriter(f, fieldnames=match_stats_csv_fields)
        dictwriter.writerow(last_scraped_match_stats)
    # except Exception as e:

    #     print("FUCK - error in save_csv()")
    #     print(e)
    #     print("\n")

def extract_map_team_stats(html : str):
    soup = BeautifulSoup(html, "html.parser")
    team_stats_rows = soup.select("div.match-info-row .right")
    team_stats = {}
    breakdown = team_stats_rows[0].text.replace(")","").replace(" ","").split("(")
    team_stats["total_rw"] = breakdown[0].split(":")  # total round wins
    team_stats["fh_rw"] = breakdown[1].split(":")  # first half round wins
    team_stats["sh_rw"] = breakdown[2].split(":")  # second half round wins
    team_stats["ot_rw"] = ["0","0"] if len(breakdown) != 4 else breakdown[3].split(":")  # overtime round wins
    team_stats["rating"] = team_stats_rows[1].text.replace(" ", "").split(":")  # team rating for the map
    team_stats["fkills"] = team_stats_rows[2].text.replace(" ", "").split(":")  # first kills
    team_stats["cw"] = team_stats_rows[2].text.replace(" ", "").split(":")  # clutches won
    map_name = (txt:=soup.select("div.match-info-box")[0].text.lower())[txt.find("map"):].split()[1]
    t1_stats = {f"t1_{variable}": team_stats[variable][0] for variable in team_stats}
    t2_stats = {f"t2_{variable}": team_stats[variable][1] for variable in team_stats}
    return t1_stats, t2_stats, map_name

def extract_team_names_and_ids(html : str):
    soup = BeautifulSoup(html, "html.parser")
    teams = {}
    teams["t1_name"] = soup.select("div.team-left a")[0]["href"].split("/")[-1]
    teams["t1_id"] = soup.select("div.team-left a")[0]["href"].split("/")[-2]
    teams["t2_name"] = soup.select("div.team-right a")[0]["href"].split("/")[-1]
    teams["t2_id"] = soup.select("div.team-right a")[0]["href"].split("/")[-2]
    return teams

def extract_stats(match_url : str, match_date : str):

    stats = {"match_url": match_url, "match_date": match_date}

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

    t1_pstats = {}
    for pn in range(1,min(len(team1_player_stats)+1,6)):
        pstats = team1_player_stats[pn-1]
        for variable in pstats:
            t1_pstats[f"t1p{pn}_{variable}"] = pstats[variable]

    t2_pstats = {}
    for pn in range(1,min(len(team2_player_stats)+1,6)):
        pstats = team2_player_stats[pn-1]
        for variable in pstats:
            t2_pstats[f"t2p{pn}_{variable}"] = pstats[variable]

    t1_map_stats, t2_map_stats, map_name = extract_map_team_stats(r.text)   
    
    # print(t1_map_stats)
    # print(t2_map_stats)
    teams_info = extract_team_names_and_ids(r.text)

    stats["map"] = map_name
    stats.update(t1_pstats)
    stats.update(t2_pstats)
    stats.update(t1_map_stats)
    stats.update(t2_map_stats)
    stats.update(teams_info)

    # print(stats)
    # print("NUM OF STATS EXTRACTED:", len(stats))



    
    # print("".join([f"\n{tuple[0]} : {tuple[1]}, " if "player" in tuple[0] else f"{tuple[0]} : {tuple[1]}, " 
    #     for tuple in player_stats]))
    # try:
    #     save_csv()
    # except:
    #     print(r.text)
    #     print(stats)

    return stats

def scrape_matches_pages():
    global last_scraped_match_stats, match_stats, completed_matches_urls, num_matches_stats_scraped
    num_matches_stats_to_scrape = input("How many matches stats do you want to scrape? ")

    if num_matches_stats_to_scrape and (num_matches_stats_to_scrape:=int(num_matches_stats_to_scrape)) > 0:
        load_matches_stats_csv()
        num_matches_stats_scraped = 0
        urls = set(matches_urls.keys())
        with alive_bar(num_matches_stats_to_scrape, bar="filling", length=30, title="Scraping... ") as bar:
            for url in urls:
                if num_matches_stats_scraped >= num_matches_stats_to_scrape:
                    break
                if not url in completed_matches_urls: 
                    match_stats.append(last_scraped_match_stats:=extract_stats(url, matches_urls[url]))
                    completed_matches_urls.append(url)
                    num_matches_stats_scraped += 1
                    save_full_csv()
                    bar() 

        print(f"number of new/unique matches stats scraped: {num_match_stats_loaded} -> {len(match_stats)}")


scrape_matches_pages()

# print(extract_team_names_and_ids(requests.get("https://www.hltv.org/stats/matches/mapstatsid/134406/faze-vs-g2").text))
# print(s:=extract_stats("https://www.hltv.org/stats/matches/mapstatsid/134406/faze-vs-g2", "27/2/22"))
# 
# print(len(s) == len(match_stats_csv_fields))

