from bs4 import BeautifulSoup
import requests
import csv
import os
import pandas as pd
from datetime import datetime

def create_directories(base_dir='player_data'):
    """Create base directory if it doesn't exist"""
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

def flatten_player_data(player_dict, position_type):
    """Flatten nested player data structure"""
    flat_player = {}
    for key, value in player_dict.items():
        if isinstance(value, dict) and 'default' in value:
            flat_player[key] = value['default']
        else:
            flat_player[key] = value
    flat_player['positionType'] = position_type
    return flat_player

def get_season_rosters(year, base_dir='player_data'):
    """
    Fetches roster data for all teams in a given season and saves to a CSV file.
    
    Args:
        year (str): Season year in format YYYYYYYY (e.g., '20232024')
        base_dir (str): Base directory for saving files
    """
    year_str = str(year)
    
    # Create season directory
    season_dir = os.path.join(base_dir, year_str[0:4])
    os.makedirs(season_dir, exist_ok=True)
    
    # Initialize list to store all roster data
    all_roster_data = []
    
    teams = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", 
             "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", 
             "NYI", "NYR", "OTT", "PHX", "PHI", "PIT", "SJS", "STL", "TBL", "TOR", 
             "VAN", "WSH", "WPG"]
    
    # Create CSV file for this season
    output_file = os.path.join(season_dir, f'rosters_{year_str}.csv')
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = None
        
        for team in teams:
            url = f"https://api-web.nhle.com/v1/roster/{team}/{year_str}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                
                # Get players from all position categories
                players = []
                for position_type in ['forwards', 'defensemen', 'goalies']:
                    if position_type in data:
                        for player in data[position_type]:
                            # Flatten nested structure and add team/season info
                            flat_player = flatten_player_data(player, position_type)
                            flat_player['season'] = year_str
                            flat_player['team'] = team
                            players.append(flat_player)
                
                # Initialize writer with the fields from the first player
                if writer is None and players:
                    fieldnames = list(players[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                
                # Write all players to CSV
                if players:
                    writer.writerows(players)
                    all_roster_data.extend(players)
                else:
                    print(f"No player data found for {team} in {year_str}")
                    
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for {team} in {year_str}: {e}")
                continue
            except KeyError as e:
                print(f"Unexpected data structure for {team} in {year_str}: {e}")
                continue
            except Exception as e:
                print(f"Unexpected error processing {team} in {year_str}: {e}")
                continue
                
    print(f"Completed processing for season {year_str}")
    return all_roster_data

def process_all_seasons(years, base_dir='player_data'):
    """
    Process multiple seasons and create CSV files for each.
    
    Args:
        years (list): List of season years to process
        base_dir (str): Base directory for saving files
    """
    create_directories(base_dir)
    all_seasons_data = []
    
    for year in years:
        print(f"\nProcessing season {year}...")
        season_data = get_season_rosters(year, base_dir)
        if season_data:
            all_seasons_data.extend(season_data)
    
    # Create combined CSV with all seasons
    if all_seasons_data:
        combined_file = os.path.join(base_dir, 'all_seasons_rosters.csv')
        df = pd.DataFrame(all_seasons_data)
        df.to_csv(combined_file, index=False)
        print(f"\nCreated combined CSV at {combined_file}")
        return df
    
    return None

# Example usage
years = [20112012, 20122013, 20132014, 20142015, 20152016, 20162017, 20172018]
all_data = process_all_seasons(years)