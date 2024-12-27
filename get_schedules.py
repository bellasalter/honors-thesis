import requests
import pandas as pd
import time

def get_team_schedule(team_code, season):
    url = f"https://api-web.nhle.com/v1/club-schedule-season/{team_code}/{season}"
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"API request failed for {team_code} {season}: {response.status_code}")
    
    data = response.json()
    print(data.get('games', []))
    games = []
    for game in data.get('games', []):
        games.append({
            'year': int(season[:4]),
            'game_id': game['id'],
            'homeTeam': game['homeTeam']['abbrev'],
            'awayTeam': game['awayTeam']['abbrev']
        })
    
    return games

def compile_schedule_data(start_year, end_year, output_file='nhl_schedule.csv'):
    teams = ['ANA', 'ARI', 'BOS', 'BUF', 'CGY', 'CAR', 'CHI', 'COL', 
             'CBJ', 'DAL', 'DET', 'EDM', 'FLA', 'LAK', 'MIN', 'MTL',
             'NSH', 'NJD', 'NYI', 'NYR', 'OTT', 'PHI', 'PIT', 'SJS',
             'SEA', 'STL', 'TBL', 'TOR', 'VAN', 'VGK', 'WSH', 'WPG']
    
    all_games = []
    for year in range(start_year, end_year + 1):
        season = f"{year}{year+1}"
        for team in teams:
            try:
                games = get_team_schedule(team, season)
                all_games.extend(games)
                time.sleep(0.5)  # Rate limiting
                print(f"got {team}")
            except Exception as e:
                print(f"Error: {str(e)}")
    
    df = pd.DataFrame(all_games)
    df = df.drop_duplicates()  # Remove duplicate games
    df.to_csv(output_file, index=False)
    return df

# Example usage
df = compile_schedule_data(2011, 2018)