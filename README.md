
# Thesis Code/Data Components

## 1. Webscraping Data Collection Pipeline in Python

* Gathers all play, shift, schedule, and roster data from the year 2011 to the year 2018 in the NHL. 
    - For the most part, contains direct data from the NHL API with *no* processing. 
* The majority of these files should not be run again. After the primary collection of the CSVs present in the database, they are obselete, but exist here so that the project is self-contained. 

### Necessary Files

#### Data Files Generated
* **combined_seasons.csv file** contains the rosters for every season in our dataset. 
    - headers 
* **play_data/ folder** stores the data for each play of every game. Contains folders named for each year, which each contain CSVs for games titled as the game id, i.e. play_data/2011/2011010001.csv. 
    - CSVs contain headers eventId,periodDescriptor,timeInPeriod,timeRemaining,situationCode,homeTeamDefendingSide,typeCode,typeDescKey,sortOrder,details,pptReplayUrl
    - note that the names are the consistent with the official game ids given by the NHL: [see game 2011010001 here](https://www.nhl.com/gamecenter/buf-vs-car/2011/09/19/2011010001).

* **player_data/ folder** stores the official NHL rosters. Contains folders named for each year, each with one CSV file containing the roster for that year, and one file with the combined rosters every year. 
    - Most important CSV is **all_seasons_rosters.csv**, which is the file actually used in attributing a team to a player when calculating the aggregate statistics. This CSV has an extra header, *season*. 
    - CSVs contain headers id,headshot,firstName,lastName,sweaterNumber,positionCode,shootsCatches,heightInInches,weightInPounds,heightInCentimeters,weightInKilograms,birthDate,birthCity,birthCountry,birthStateProvince,positionType,season,team


* **shift_data/ folder** stores the data for every shift of every game. Contains folders named for each year, which each contain CSVs for games titled as the game id, i.e. play_data/2011/2011010001.csv. 
    - CSVs contain headers gameId,teamAbbrev,firstName,lastName,period,startTime,endTime,shiftNumber,eventDescription,eventDetails,typeCode,teamId,hexValue,detailCode,playerId,teamName,id,teamAbbrev,eventNumber,duration
    - note that playerId may be of the form 1234567/8910112/, etc. due to multiple player shift changes

#### Computation

1.  **webscraping.py** does the inital webscraping from the NHL API. It generates the folders and CSVs under play_data and shift_data. **important notes**: 
    - end state: **play_data/** and **shift_data/** populated with data
        - *Will not run* if the necessary CSVs exist on your device. This is primarily to prevent overwriting data, but also prevents a long runtime when the data is already present. 
    - webscraping.py takes a long time to run. It could be parallelized, but the tradeoff isn't worth it because it stores the CSVs and doesn't need to be run again.
    - not every game with play data has shift data, as shift data collection is relatively new. 

2. **getAllPlayers.py** gets the rosters for each year in the NHL. 
    - end state: **player_data/** populated with data.
        - *Will not run* if the necessary CSVs exist on your device. This is primarily to prevent overwriting data, but also prevents a long runtime when the data is already present. 
    - Runtime is not very significant, but data files are still already provided. 

3. **consolidate.py** consolidates the roster data for each year into one csv, from the preliminary data that **getAllPlayers.py** generated.
    - Runtime is not very significant, but data files are still already provided. 

## 2. Data Processing in R
