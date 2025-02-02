from bs4 import BeautifulSoup
import requests
import csv
import os
import pandas as pd

# GLOBAL VARIABLES
teams = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR", "OTT", "PHI", "PHX", "PIT", "SJS", "STL", "TBL", "TOR", "VAN", "WSH", "WPG"]

# gets the boxscore data for that game in that year
# Params: gameID, the formatted game id
#         year, the year of the game, used for file location
#         first, to know whether or not to make a directory
# Returns: 1 if successful
def get_box_data(gameID, year, firstBox) :
    #os.mkdir(f'./shift_data/{year}', )
    game_no = f"https://api-web.nhle.com/v1/gamecenter/{gameID}/play-by-play"

    r = requests.get(game_no)
    #print(r.json()['plays'][0].keys())
    if r.status_code != 200 :
        return [0,0]
    #for play in r.json()['plays'] :
        #print(play['typeDescKey'])
    #if(r.json()['total'] == 0) :
        #return 0
    
    if firstBox == 0:
        #print(firstBox)
        os.mkdir(f'data_collection/play_data/{year}', )

    goals = r.json()['plays']
    #for row in r.json()['plays'] :
        #if row['typeDescKey'] == "goal":
            
            #goals.append(row)
            #print(row)



    with open(f'data_collection/play_data/{year}/{gameID}.csv', 'w', newline='') as csvfile:
        fieldnames = ['eventId', 'periodDescriptor', 'timeInPeriod', 'timeRemaining', 'situationCode', 'homeTeamDefendingSide', 'typeCode', 'typeDescKey', 'sortOrder', 'details', 'pptReplayUrl']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(goals)
    return [1,goals]


# gets the shift data for that game in that year
# Params: gameID, the formatted game id
#         year, the year of the game, used for file location
#         first, to know whether or not to make a directory
# Returns: 1 if successful
def get_shift_data(gameID, year, first) :
    #os.mkdir(f'./shift_data/{year}', )
    game_no = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gameID}"

    r = requests.get(game_no)
    if(r.json()['total'] == 0) :
        return [0,0]
    
    if first == 0:
        os.mkdir(f'data_collection/shift_data/{year}', )
    
    #goals = [row for row in r.json()['data'] if row["typeDesc

    with open(f'data_collection/shift_data/{year}/{gameID}.csv', 'w', newline='') as csvfile:
        fieldnames = ['gameId','teamAbbrev','firstName', 'lastName', 'period','startTime', 'endTime', 'shiftNumber', 'eventDescription', 'eventDetails', 'typeCode', 'teamId', 'hexValue', 'detailCode', 'playerId', 'teamName', 'id', 'teamAbbrev', 'eventNumber', 'duration']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(r.json()['data'])
    return [1,r.json()['data']]

# gets the game ids for that year
# Params: year, of the form 20012002, used to get the url for the schedule
# Returns: a list of the unique game ids for that year
def get_all_game_ids(year) :
    games = []
    for team in teams:
        season = year
        r3 = requests.get(f"https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}")
        for game in r3.json()['games'] :
            if game['id'] not in games:
                games.append(game['id'])
    return games

    
def main():
    r2 = requests.get("https://api-web.nhle.com/v1/season")

    start_ind = r2.json().index(20112012) # this is when shift charts started
    for szn in r2.json()[start_ind:] :
        szn = str(szn)
        print(f"{szn[0:4]}-{szn[4:]} Season -----------")
        gameIDs = get_all_game_ids(szn)
        print(f"{len(gameIDs)} games found.")
        count = 0
        countBox = 0
        for game in gameIDs:
            #print(count)
            success = get_shift_data(game,szn[0:4], count)
            successBox = get_box_data(game,szn[0:4],countBox)
            countBox += successBox[0]
            count += success[0] # need to know whether to make directory
            shift_df = success[1]
            box_df = successBox[1]
            #if count == 50:
                #return
            #if success == 0:
                #continue
            #create_game_summary(shift_df, box_df, szn[0:4], game, count)

        
        print(f"{count} games found with shift data.")

def create_game_summary(shift_df_old, box_df, yr, gameID, count) :
    if count == 0:
        os.mkdir(f"summary/{yr}")

    new_df_b = pd.DataFrame(box_df)
    #print(new_df_b)
    new_df_b["type"] = "goal"

    new_df = []
    print(shift_df_old)
    shift_df = pd.DataFrame(shift_df_old)
    # consider entering same time different period....
    for val in shift_df["startTime"].unique() :
        curr_players = {
            "firstName" : [],
            "lastName" : [],
        }
        period = 1
        for row in shift_df:
            if row["startTime"] == val:
                period = row["period"]
                curr_players["firstName"].append(row["firstName"])
                curr_players["lastName"].apppend(row["lastName"])
        curr_val = {
            "periodDescriptor":period,
            "details": curr_players,
            "timeInPeriod": val
        }
        new_df.append(curr_val)
    new_df["type"] = "shift"
    new_df["eventID"] = 00
    new_df["timeRemaining"] = 00
    new_df["situationCode"] = 00
    new_df["homeTeamDefendingSide"] = 00
    new_df["typeCode"] = 00
    new_df["typeDescKey"] = 00
    new_df["sortOrder"] = 00

    with open(f'summary/{yr}/{gameID}.csv', 'w', newline='') as csvfile:
        fieldnames = ['eventId', 'periodDescriptor', 'timeInPeriod', 'timeRemaining', 'situationCode', 'homeTeamDefendingSide', 'typeCode', 'typeDescKey', 'sortOrder', 'details', 'pptReplayUrl']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(new_df)
        writer.writerows(new_df_b)
    return 1

    
main()
#shift_dir = "shift_data/"
#box_dir = "play_data/"
#for file in os.listdir(shift_dir): 
    #os.mkdir(f"summary/{file}")
    #for game in os.listdir(shift_dir+file):
        #create_game_summary(f"shift_data/{game[0:4]}/{game}",f"play_data/{game[0:4]}/{game}")



