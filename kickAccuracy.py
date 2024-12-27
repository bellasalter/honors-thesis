import sys
import nfl_data_py as nfl
from collections import defaultdict

# Create a defaultdict for field goal success rates, indexed by distance.
# Each entry should be a tuple (fgAttempts, fgMade) from that distance.
# Note that with defaultdict, missing keys are initialized with (0, 0)

fgDict = defaultdict(lambda: (0, 0))

# pbp is a Pandas object
pbp = nfl.import_pbp_data([2021,2022,2023], downcast=True, cache=False, alt_path=None)

for index, row in pbp.iterrows():   # iterates through every play
  playType = row["play_type"]
  if playType != "field_goal":  # only consider field goals
    continue
  # now get the distance and result through fields "kick_distance"
  # and "field_goal_result"
  result = row["field_goal_result"]
  distance = row["kick_distance"]
  # add this distance to the dictionary; increment the attempts in the
  # proper tuple by 1, and increase the makes by 1 if the FG was made
  # remember that tuples in Python are immutable
  #distance = 0
  oldTuple = fgDict[distance]
  if result == "made":
    fgDict[distance] = (oldTuple[0] + 1, oldTuple[1] + 1)
  else:
    fgDict[distance] = (oldTuple[0] + 1, oldTuple[1])


# sort dictionary by distance
sorted_items = sorted(fgDict.items(), key=lambda x: x[0])
sorted_items = sorted_items[1:]   # get rid of 19 yard FGs

# now write a loop over the tuples to generate the success
# rate by five yard increments; so, 20-24 yarders, 25-29 yarders, etc.

count = 24
curr_tot_made = 0
curr_tot_attempt = 0
for t in sorted_items:
  if(t[0] > count) :
    print(f"{count - 4}-{count} yard lines: {curr_tot_made} out of {curr_tot_attempt}: {curr_tot_made / curr_tot_attempt}")
    count += 5
    curr_tot_made = 0
    curr_tot_attempt = 0
    continue
  curr_tot_made = curr_tot_made + t[1][1]
  curr_tot_attempt = curr_tot_attempt + t[1][0]