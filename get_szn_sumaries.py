import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from typing import List, Dict
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NHLStatsScraper:
    def __init__(self, start_year: int, end_year: int):
        self.start_year = start_year
        self.end_year = end_year
        self.base_url = "https://www.hockey-reference.com/leagues/NHL_{}.html"
        
    def get_page_content(self, year: int) -> str:
        """Fetch the HTML content for a given year."""
        url = self.base_url.format(year)
        
        try:
            # Add headers to mimic browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logging.error(f"Error fetching data for year {year}: {e}")
            return None

    def parse_team_stats(self, html_content: str, year: int) -> pd.DataFrame:
        """Parse the HTML content and extract team statistics."""
        if not html_content:
            return pd.DataFrame()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the teams table
        table = soup.find('table', {'id': 'stats'})
        if not table:
            logging.warning(f"No stats table found for year {year}")
            return pd.DataFrame()
        
        # Parse the table into a DataFrame
        df = pd.read_html(str(table))[0]
        
        # Clean up the DataFrame
        # Remove any multi-level column headers
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)
        
        # Remove rows that are actually header rows repeated in the middle
        df = df[df['Rk'] != 'Rk']
        
        # Add season column
        df['Season'] = year
        
        # Remove any asterisks from team names if they exist
        if 'Team' in df.columns:
            df['Team'] = df['Team'].str.replace('*', '', regex=False)
        
        return df

    def scrape_all_seasons(self) -> pd.DataFrame:
        """Scrape data for all seasons in the specified range."""
        all_seasons_data = []
        
        for year in range(self.start_year, self.end_year + 1):
            logging.info(f"Scraping data for season {year}")
            
            # Get the page content
            html_content = self.get_page_content(year)
            
            # Parse the content
            season_df = self.parse_team_stats(html_content, year)
            
            if not season_df.empty:
                all_seasons_data.append(season_df)
            
            # Be nice to the server with a delay between requests
            time.sleep(2)
        
        # Combine all seasons into one DataFrame
        if all_seasons_data:
            final_df = pd.concat(all_seasons_data, ignore_index=True)
            
            # Select and rename columns as needed
            columns_to_keep = [
                'Season', 'Team', 'GP', 'W', 'L', 'OL', 'PTS', 'GF', 'GA', 
                'PIM', 'PP', 'PPO', 'PP%', 'PK%', 'SRS'
            ]
            
            # Only keep columns that exist in the DataFrame
            final_cols = [col for col in columns_to_keep if col in final_df.columns]
            final_df = final_df[final_cols]
            
            return final_df
        
        return pd.DataFrame()

    def save_to_csv(self, df: pd.DataFrame, filename: str = 'nhl_stats.csv'):
        """Save the DataFrame to a CSV file."""
        df.to_csv(filename, index=False)
        logging.info(f"Data saved to {filename}")

def main():
    # Example usage
    start_year = 1990  # Adjust start year as needed
    end_year = 2024    # Adjust end year as needed
    
    scraper = NHLStatsScraper(start_year, end_year)
    
    logging.info("Starting NHL stats scraping...")
    df = scraper.scrape_all_seasons()
    
    if not df.empty:
        scraper.save_to_csv(df)
        logging.info(f"Successfully scraped data from {start_year} to {end_year}")
    else:
        logging.error("No data was scraped")

if __name__ == "__main__":
    main()