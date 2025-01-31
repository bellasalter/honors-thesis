import pandas as pd
import os
from typing import List, Optional
import logging
from pathlib import Path

class SeasonDataConsolidator:
    def __init__(self, data_folder: str = "team_data"):
        """
        Initialize the consolidator with the folder path containing season CSV files.
        
        Args:
            data_folder (str): Path to the folder containing the CSV files
        """
        self.data_folder = Path(data_folder)
        self.setup_logging()

    def setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def extract_season(self, filename: str) -> str:
        """
        Extract season from filename (e.g., "20112012.csv" -> "2011-2012")
        
        Args:
            filename (str): Name of the CSV file
            
        Returns:
            str: Formatted season string
        """
        # Remove .csv extension
        season_nums = filename.replace('.csv', '')
        
        # Extract start and end years
        if len(season_nums) == 8:  # Assuming format "20112012"
            start_year = season_nums[:4]
            end_year = season_nums[4:]
            return f"{start_year}-{end_year}"
        else:
            self.logger.warning(f"Unexpected filename format: {filename}")
            return filename

    def read_csv_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Read a single CSV file and add season information.
        
        Args:
            file_path (Path): Path to the CSV file
            
        Returns:
            Optional[pd.DataFrame]: DataFrame with the file's data, or None if there's an error
        """
        try:
            df = pd.read_csv(file_path)
            season = self.extract_season(file_path.name)
            df['Season'] = season
            return df
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            return None

    def get_csv_files(self) -> List[Path]:
        """
        Get list of all CSV files in the data folder.
        
        Returns:
            List[Path]: List of paths to CSV files
        """
        return sorted(self.data_folder.glob('*.csv'))

    def combine_data(self) -> pd.DataFrame:
        """
        Combine all CSV files in the data folder into a single DataFrame.
        
        Returns:
            pd.DataFrame: Combined DataFrame with all seasons' data
        """
        # Check if folder exists
        if not self.data_folder.exists():
            raise FileNotFoundError(f"Folder '{self.data_folder}' not found")

        # Get list of CSV files
        csv_files = self.get_csv_files()
        
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in '{self.data_folder}'")

        # Read and combine all CSV files
        self.logger.info("Starting to combine CSV files...")
        dfs = []
        
        for file_path in csv_files:
            self.logger.info(f"Processing {file_path.name}")
            df = self.read_csv_file(file_path)
            if df is not None:
                dfs.append(df)

        if not dfs:
            raise ValueError("No valid data found in any CSV files")

        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Sort by season and any other relevant columns
        if 'Team' in combined_df.columns:
            combined_df = combined_df.sort_values(['Season', 'Team'])
        else:
            combined_df = combined_df.sort_values('Season')

        self.logger.info(f"Successfully combined {len(dfs)} CSV files")
        return combined_df

    def save_combined_data(self, df: pd.DataFrame, output_file: str = "combined_seasons.csv"):
        """
        Save the combined DataFrame to a CSV file.
        
        Args:
            df (pd.DataFrame): DataFrame to save
            output_file (str): Name of the output file
        """
        try:
            df.to_csv(output_file, index=False)
            self.logger.info(f"Combined data saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Error saving combined data: {str(e)}")

def main():
    """Main function to run the consolidation process."""
    try:
        # Initialize the consolidator
        consolidator = SeasonDataConsolidator("team_data")
        
        # Combine the data
        combined_df = consolidator.combine_data()
        
        # Print some basic information about the combined dataset
        print("\nDataset Summary:")
        print(f"Total number of rows: {len(combined_df)}")
        print(f"Columns: {', '.join(combined_df.columns)}")
        print(f"\nUnique seasons: {sorted(combined_df['Season'].unique())}")
        
        # Save the combined data
        consolidator.save_combined_data(combined_df)
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")

if __name__ == "__main__":
    main()
