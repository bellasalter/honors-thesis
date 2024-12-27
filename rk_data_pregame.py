import requests
import pandas as pd
import time
import os
from typing import Dict, List, Tuple
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_team_codes_for_season(season: int) -> List[str]:
    """Return appropriate team codes based on season."""
    base_teams = [
        'BOS', 'BUF', 'CGY', 'CAR', 'CHI', 'COL', 'CBJ', 'DAL', 
        'DET', 'EDM', 'FLA', 'LAK', 'MIN', 'MTL', 'NSH', 'NJD', 'NYI', 'NYR', 
        'OTT', 'PHI', 'PIT', 'SJS', 'STL', 'TBL', 'TOR', 'VAN', 'WSH', 'WPG'
    ]
    
    # Handle Arizona/Phoenix changes
    if season < 2014:  # Phoenix Coyotes (before 2014-15 season)
        base_teams.append('PHX')
    else:  # Arizona Coyotes (2014-15 season and after)
        base_teams.append('ARI')
    
    # Handle Anaheim name changes (though code stayed as ANA)
    base_teams.append('ANA')
    
    # Add Vegas Golden Knights (entered league in 2017-18)
    if season >= 2017:
        base_teams.append('VGK')
    
    return base_teams

def ensure_directory_exists():
    if not os.path.exists('rankings_data'):
        os.makedirs('rankings_data')

def create_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def api_request(url: str, max_retries: int = 3) -> Dict:
    session = create_session()
    for attempt in range(max_retries):
        try:
            print(f"Making request to: {url}")
            response = session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            print(f"Got response with keys: {data.keys() if data else 'No data'}")
            return data
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed for URL {url}: {str(e)}")
            if attempt == max_retries - 1:
                print(f"Failed to fetch {url} after {max_retries} attempts")
                return {}
            time.sleep(2 ** attempt)

def get_season_games(team_code: str, season: int) -> List[Dict]:
    print(f"\nGetting games for {team_code} season {season}")
    season_str = f"{season}{season+1}"
    url = f"https://api-web.nhle.com/v1/club-schedule-season/{team_code}/{season_str}"
    data = api_request(url)
    games = data.get('games', [])
    print(f"Found {len(games)} games for {team_code}")
    return games

def get_boxscore(game_id: str) -> Dict:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
    data = api_request(url)
    return data

def process_game_data(game: Dict, boxscore: Dict, season: int) -> List[Dict]:
    results = []
    if not boxscore:
        print(f"Invalid boxscore data for game {game.get('id', 'unknown')}")
        return results
    
    game_date = game['gameDate']
    game_season = int(game_date.split('-')[0])
    
    print(f"Processing game from date: {game_date}")
    
    # Get home and away team data from the new structure
    away_team = boxscore.get('awayTeam', {})
    home_team = boxscore.get('homeTeam', {})
    
    if not away_team or not home_team:
        print("Missing team data in boxscore")
        return results
    
    # Calculate PIM safely
    def calculate_pim(team_stats):
        try:
            players = team_stats.get('players', {}).values()
            return sum(player.get('stats', {}).get('pim', 0) for player in players)
        except Exception as e:
            print(f"Error calculating PIM: {e}")
            return 0
    
    # Process away team
    away_data = {
        'game_id': game['id'],
        'date': game_date,
        'season': season,
        'team_id': away_team.get('id', ''),
        'team_code': away_team.get('abbrev', ''),
        'team_type': 'away',
        'goals': away_team.get('score', 0),
        'shots': away_team.get('shotsOnGoal', 0),
        'pim': calculate_pim(boxscore.get('playerByGameStats', {}).get('awayTeam', {})),
        'powerPlayGoals': away_team.get('powerPlayGoals', 0),
        'powerPlayOpportunities': away_team.get('powerPlayOpportunities', 0),
        'blocked': away_team.get('blockedShots', 0),
        'takeaways': away_team.get('takeaways', 0),
        'giveaways': away_team.get('giveaways', 0),
        'hits': away_team.get('hits', 0)
    }
    
    # Process home team
    home_data = {
        'game_id': game['id'],
        'date': game_date,
        'season': season,
        'team_id': home_team.get('id', ''),
        'team_code': home_team.get('abbrev', ''),
        'team_type': 'home',
        'goals': home_team.get('score', 0),
        'shots': home_team.get('shotsOnGoal', 0),
        'pim': calculate_pim(boxscore.get('playerByGameStats', {}).get('homeTeam', {})),
        'powerPlayGoals': home_team.get('powerPlayGoals', 0),
        'powerPlayOpportunities': home_team.get('powerPlayOpportunities', 0),
        'blocked': home_team.get('blockedShots', 0),
        'takeaways': home_team.get('takeaways', 0),
        'giveaways': home_team.get('giveaways', 0),
        'hits': home_team.get('hits', 0)
    }
    
    results.extend([away_data, home_data])
    print(f"Added data for {away_data['team_code']} (away) and {home_data['team_code']} (home)")
    
    return results

def calculate_standings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values('date')
    team_points = {team: 0 for team in df['team_code'].unique()}  # Use actual teams from data
    team_games = {team: 0 for team in df['team_code'].unique()}
    
    def calculate_rank(row):
        game_df = df[df['game_id'] == row['game_id']]
        if len(game_df) != 2:
            return None
            
        home_team = game_df[game_df['team_type'] == 'home'].iloc[0]
        away_team = game_df[game_df['team_type'] == 'away'].iloc[0]
        
        team_games[home_team['team_code']] += 1
        team_games[away_team['team_code']] += 1
        
        if home_team['goals'] > away_team['goals']:
            team_points[home_team['team_code']] += 2
        elif away_team['goals'] > home_team['goals']:
            team_points[away_team['team_code']] += 2
        else:
            team_points[home_team['team_code']] += 1
            team_points[away_team['team_code']] += 1
        
        points_pct = {team: (points / (team_games[team] * 2)) if team_games[team] > 0 else 0 
                     for team, points in team_points.items()}
        
        sorted_teams = sorted(points_pct.items(), key=lambda x: x[1], reverse=True)
        team_ranks = {team: rank + 1 for rank, (team, _) in enumerate(sorted_teams)}
        
        return team_ranks[row['team_code']]
    
    df['rank'] = df.apply(calculate_rank, axis=1)
    return df

def collect_single_season(season: int):
    print(f"\nStarting collection for season {season}")
    season_games_data = []
    processed_games = set()
    
    team_codes = get_team_codes_for_season(season)
    for team_code in team_codes:
        print(f"\nProcessing team {team_code} for season {season}")
        games = get_season_games(team_code, season)
        
        for game in games:
            game_id = game['id']
            if game_id in processed_games:
                print(f"Skipping already processed game {game_id}")
                continue
                
            try:
                print(f"Getting boxscore for game {game_id}")
                boxscore = get_boxscore(game_id)
                game_data = process_game_data(game, boxscore, season)
                if game_data:
                    season_games_data.extend(game_data)
                    processed_games.add(game_id)
                    print(f"Successfully processed game {game_id}")
                time.sleep(1)
                
            except Exception as e:
                print(f"Error processing game {game_id}: {str(e)}")
                continue
    
    print(f"\nCollected data for {len(processed_games)} games")
    if season_games_data:
        season_df = pd.DataFrame(season_games_data)
        print(f"Created DataFrame with {len(season_df)} rows")
        print(f"Calculating rankings for season {season}-{season+1}...")
        season_df = calculate_standings(season_df)
        output_path = f'rankings_data/season_{season}_{season+1}.csv'
        season_df.to_csv(output_path, index=False)
        print(f"Saved season {season}-{season+1} to {output_path}")
        return season_df
    else:
        print("No data collected for this season")
    return None

def collect_season_data(start_year: int, end_year: int):
    ensure_directory_exists()
    
    for year in range(start_year, end_year + 1):
        print(f"\nProcessing season {year}-{year+1}")
        collect_single_season(year)

if __name__ == "__main__":
    collect_season_data(2011, 2018)