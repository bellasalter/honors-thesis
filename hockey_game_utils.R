library(data.table)
team_info_colnames <- c("Szn", "tmAbbrev", "W", "L", "SOG", "Wrist", "Slap", "Snap", "Tip", "Deflect", "Missed", "Blocked", "Goals")
play_info_colnames <- c("PlayerId","Pos", "Szn", "tmAbbrev", "SOG", "Wrist", "Slap", "Snap", "Tip", "Deflect", "Missed", "Blocked", "Goals")
new_play_colnames <- c("PlayerId","Pos", "Szn", "tmAbbrev", "SOG", "Wrist", "Slap", "Snap", "Tip", "Deflect", "Missed", "Blocked", "Goals", "line1", "line2")
schedule_info <- read_csv("./nhl_schedule.csv")


# Gets all relevant info and puts this into a csv
get_all_info <- function(year, all_game_ids, debug=FALSE) {
  count = 0
  play_info_colnames <- c("PlayerId", "Szn", "Pos", "shootsCatches", "tmAbbrev", "SOG", "Wrist", "Slap", "Snap", "Tip", "Deflect", "Missed", "Blocked", "Goals", "Hits")
  
  big_team_df <- data.frame(matrix(nrow = 0, ncol = length(team_info_colnames)))
  big_player_df <- data.frame(matrix(nrow = 0, ncol = length(play_info_colnames)))
  colnames(big_team_df) <- team_info_colnames
  colnames(big_player_df) <- play_info_colnames
  curr_yr <- "2012"
  start_time <- Sys.time()
  game_id_df <- data.frame(all_game_ids)
  
  for(game in game_ids) {
    if(debug==TRUE) {
      print(game)
      if(count > 5) {
        break
      }
    } 
    shift_path = sprintf("shift_data/%s/%s", substring(game, 1, 4), game)
    play_path = sprintf("play_data/%s/%s", substring(game, 1, 4), game)
    
    
    if(!(file.exists(file = shift_path)) | !(file.exists(file=play_path))) {
      next
    }
    #print(shift_path)
    shift_data = read_csv(shift_path, col_types = cols())
    
    
    
    if(substring(game, 1, 4) != curr_yr) {
      end_time <- Sys.time()
      diff <- end_time - start_time
      print(sprintf("Processing of %s seasons complete. Time elapsed %s", curr_yr, diff))
      start_time <- Sys.time()
      curr_yr <- substring(game, 1,4)
    }
    # dealing with shift data first....
    shift_data = subset(shift_data, select = -c(eventDescription, eventDetails, eventNumber, hexValue, detailCode))
    #print(colnames(shift_data))
    colnames(shift_data) = c("gameId", "teamAbbrev", "firstName", "lastName", "period", "startTime", "endTime", "shiftNumber","typeCode","teamId","playerId","teamName","id","opTeamAbbrev","duration")
    shift_data$playerName = paste(shift_data$firstName, shift_data$lastName, sep="")
    
    # get individual player info and team
    #szn <- rep()
    shift_data <- na.omit(shift_data)
    player_info <- data.frame(unique(shift_data$playerId), unique(shift_data$playerName))
    player_teams <- c()
    player_positions <- c()
    colnames(player_info) <- c("playerId", "playerName")
    #write.csv(player_info,"/players.csv", row.names = FALSE)
    
    for(player in player_info$playerName) {
      player_shifts = shift_data[shift_data$playerName == player,] 
      player_team <- player_shifts$teamAbbrev[1]
      player_teams <- c(player_teams, player_team)
      
    }
    
    player_scs <- c()
    for(player in player_info$playerId) {
      pos <- complete_roster[complete_roster$season == curr_yr & complete_roster$id == player,]
      new_pos <- pos$positionCode[1]
      player_positions <-c(player_positions, new_pos)#EDITING HERE
      new_scs <- pos$shootsCatches[1]
      player_scs <- c(player_scs, new_scs)
    }
    player_info$teamAbbrev <- player_teams
    player_info$Pos <- player_positions
    player_info$shootsCatches <- player_scs
    #print(player_info)
    
    shift_data = subset(shift_data, select = -c(firstName,lastName, typeCode, teamName, opTeamAbbrev, id))
    shift_data$startTimeI = sprintf("P%s-%s", shift_data$period,shift_data$startTime)
    
    tm_info <- c()
    num_players <- c()
    player_ids <- c()
    player_names<- c()
    start_times <- c()
    for(shift in unique(shift_data$startTimeI)) {
      rows_time <- shift_data[shift_data$startTimeI == shift,]
      for(team in unique(rows_time$teamAbbrev)) {
        start_times <- c(start_times, shift)
        tm_info <- c(tm_info, team)
        tm_rows = rows_time[rows_time$teamAbbrev == team,]
        num_players <- c(num_players,nrow(tm_rows))
        players_str <- ""
        players_names <- ""
        #print(tm_rows)
        for(i in 1:nrow(tm_rows)){
          row = tm_rows[i,]
          #print(row)
          players_str <- paste(players_str, row$playerId, sep="")
          players_str <- paste(players_str, "/", sep="")
          players_names <- paste(players_names, row$playerName, sep="")
          players_names <- paste(players_names, "/", sep="")
        }
        player_names <- c(player_names, players_names)
        player_ids <- c(player_ids, players_str)
      }
      
    }
    if(length(start_times) == 0) {
      print("START TIME IS EMPTY")
    }
    
    if(length(tm_info) == 0) {
      print("TM_INFO EMOPTY")
    }
    
    if(length(num_players) == 0) {
      print("NUM PLAYERS EMOPTY")
    }
    new_df <- data.frame(start_times, tm_info, num_players, player_ids)#, player_names)
    colnames(new_df) <- c("TOI", "tmAbbrev", "numPlayers", "playerIDs")#, "playerNames")
    new_df$type <- rep("shift-change", nrow(new_df))
    new_df$details <- rep("", nrow(new_df))
    new_df$situationCode <- rep(0000, nrow(new_df))
    new_df$zoneCode <- rep(0, nrow(new_df))
    
    
    #print(new_df)
    
    # now dealing with play data....
    play_data = read_csv(play_path, col_types = cols())
    play_data$type = play_data$typeDescKey
    play_data = subset(play_data, select = -c(pptReplayUrl, sortOrder, typeCode, homeTeamDefendingSide, eventId,typeDescKey))
    #print(play_data$periodDescriptor) 
    play_data$periodDescriptor = substring(play_data$periodDescriptor, 12, 12)
    play_data$TOI = sprintf("P%s-%s", play_data$periodDescriptor,play_data$timeInPeriod)
    play_data = subset(play_data, select = -c(periodDescriptor, timeInPeriod,timeRemaining))
    
    if( "shootout-complete" %in% unique(play_data$type)) {
      next
    }
    
    playerIDs_col <- c()
    zoneCodes_col <- c()
    tmAbbrevs_col <- c()
    types_col <- c()
    details <- c()
    tm1 <- "000"
    tm2 <- "000"
    
    for(i in 1:nrow(play_data)) {
      row = play_data[i,]
      deets = row$details
      deets_add <- ""
      prim_player <- ""
      if(row$type == "faceoff") {
        find_winning_player = process_json(deets, "winningPlayerId")
        playerIDs_col <- c(playerIDs_col,find_winning_player)
        
        prim_player <- find_winning_player
        find_zone_code <- process_json(deets, "zoneCode")
        
        tmAbbrev <- complete_roster[complete_roster$season == curr_yr & complete_roster$id == prim_player,]$team
        #print(tmAbbrev)
        
        #tmAbbrev <- player_info[player_info$playerId == prim_player,]$teamAbbrev
        tmAbbrevs_col <- c(tmAbbrevs_col, tmAbbrev)
        
        #if(curr_yr == "2013") {
        #print("ayo")
        #}
      } else if(row$type == "shot-on-goal" | row$type == "missed-shot" | row$type == "blocked-shot") {
        
        shooting_player = process_json(deets, "shootingPlayerId")
        playerIDs_col <- c(playerIDs_col,shooting_player)
        deets_add <- process_json(deets, "shotType")
        prim_player = shooting_player
        find_zone_code <- process_json(deets, "zoneCode")
        
        tmAbbrev <- player_info[player_info$playerId == prim_player,]$teamAbbrev
        tmAbbrevs_col <- c(tmAbbrevs_col, tmAbbrev)
        #print(sprintf("%s type %s team", row$type, tmAbbrev))
      } else if(row$type == "goal") {
        
        prim_player = process_json(deets, "scoringPlayerId")
        assist1 = process_json(deets, "assist1PlayerId")
        assist2 = process_json(deets, "assist2PlayerId")
        players = sprintf("%s/%s/%s", prim_player, assist1, assist2)
        find_zone_code <- process_json(deets, "zoneCode")
        tmAbbrev <- player_info[player_info$playerId == prim_player,]$teamAbbrev
        tmAbbrevs_col <- c(tmAbbrevs_col, tmAbbrev)
        playerIDs_col <- c(playerIDs_col,players)
        deets_add <- process_json(deets, "shotType")
        
      } else if(row$type == "hit") {
        
        hitting_player = process_json(deets, "hittingPlayerId")
        playerIDs_col <- c(playerIDs_col, hitting_player)
        deets_add <- process_json(deets, "hitteePlayerId")
        prim_player = hitting_player
        find_zone_code <- process_json(deets, "zoneCode")
        
        tmAbbrev <- player_info[player_info$playerId == prim_player,]$teamAbbrev
        tmAbbrevs_col <- c(tmAbbrevs_col, tmAbbrev)
        
      } else if(row$type == "penalty") {
        
        hitting_player = process_json(deets, "committedByPlayerId")
        if(hitting_player == "") {
          hitting_player = process_json(deets, "servedByPlayerId")
          #print(hitting_player)
        }
        playerIDs_col <- c(playerIDs_col, hitting_player)
        deets_add <- process_json(deets, "typeCode")
        prim_player = hitting_player
        find_zone_code <- "00"
        tmAbbrev <- player_info[player_info$playerId == prim_player,]$teamAbbrev
        
        if (process_json(deets, "descKey") == 'game-misconduct-team-staff' | process_json(deets, "descKey") == 'game-misconduct-head-coach') {
          tmAbbrev <- "000"
        }
        
        tmAbbrevs_col <- c(tmAbbrevs_col, tmAbbrev)
        
        
      }else if(row$type == "stoppage" |  row$type == "period-start" | row$type == "period-end" | row$type == "game-end") {
        prim_player <- 0000000
        playerIDs_col <- c(playerIDs_col, prim_player)
        find_zone_code <- process_json(deets, "zoneCode")
        tmAbbrev <- "000"
        tmAbbrevs_col <- c(tmAbbrevs_col, tmAbbrev)
        #print(sprintf("%s type %s team", row$type, tmAbbrev))
        
        
      } else if(row$type == "giveaway" | row$type == "takeaway") {
        losing_player = process_json(deets, "playerId")
        playerIDs_col <- c(playerIDs_col, losing_player)
        
        prim_player = losing_player
        find_zone_code <- process_json(deets, "zoneCode")
        tmAbbrev <- player_info[player_info$playerId == prim_player,]$teamAbbrev
        tmAbbrevs_col <- c(tmAbbrevs_col, tmAbbrev)
        
      } else {
        print(" ERROR - MISSED ROW TYPE")
        print(row$type)
        print(row)
      }
      
      game_id <-  substr(game,1,nchar(game)-4)
      curr_schedule <- schedule_info[schedule_info$game_id == game_id,]
      #print(game_id)
      tm1 <- curr_schedule$homeTeam
      tm2 <- curr_schedule$awayTeam
      zoneCodes_col <- c(zoneCodes_col, find_zone_code)
      
      details <- c(details,deets_add)
      
      
      types_col <- c(types_col, row$type)
      
    }
    
    small_df_pbp <- data.frame(play_data$TOI, playerIDs_col)
    #print(nrow(small_df_pbp))
    
    
    tmAbbrevs_col <- compile_team_abbrevs(small_df_pbp, tm1, tm2, curr_yr)
    numPlayers <- rep(0, length(tmAbbrevs_col))
    
    new_df_pbp <- data.frame(play_data$TOI, tmAbbrevs_col, numPlayers, playerIDs_col,types_col, details,play_data$situationCode, zoneCodes_col)
    
    colnames(new_df_pbp) <- colnames(new_df)
    final_df = rbind(new_df, new_df_pbp) 
    yr = substring(game, 1, 4)

    big_team_df <- get_game_counts(big_team_df, final_df,yr , tm1, tm2)
    big_player_df <- process_shot_stats(final_df, big_player_df, yr)
    yr_col <- rep(yr, nrow(final_df))
    game_id_col <- rep(game_id, nrow(final_df))
    add_to_big_df <- final_df
    add_to_big_df$yr <- yr_col
    add_to_big_df$game_id <- game_id_col
    if(count == 0) {
      giant_df <- add_to_big_df
    } else {
      giant_df <- rbind(giant_df, add_to_big_df)
    }
    
    count = count + 1
  }
  if(debug == TRUE) {
    print(big_team_df)
  }
  file.create(sprintf("./%s_df/big_player_df.csv", year))
  file.create(sprintf("./%s_df/big_team_df.csv", year))
  file.create(sprintf("./%s_df/giant_df.csv", year))
  write.csv(big_player_df, file=sprintf("./%s_df/big_player_df.csv", year))
  write.csv(big_team_df, file=sprintf("./%s_df/big_team_df.csv", year))
  write.csv(giant_df, file=sprintf("./%s_df/giant_df.csv", year))
}

get_player_profile <- function(all_plays, player_counts, id) {
  ret_df <- data.frame(matrix(nrow = 0, ncol = length(new_play_colnames)))
  colnames(ret_df) <- play_info_colnames
  shift_changes <- all_plays[all_plays$type == 'shift-change',]
  player_plays <- shift_changes[shift_changes$playerIDs %like% id, ] 
  
  for(y in c(1:length(unique(player_plays$year)))) {
    print(unique(player_plays$yr))
    yr <- unique(player_plays$yr)[y]
    #print(sprintf("YR: %s",yr))
    plays_yr <- player_plays[player_plays$yr == yr,]
    new_row <- player_counts[player_counts$Szn == yr & player_counts$PlayerId == id, ]
    linemates <- c()
    
    for(n in c(1:nrow(plays_yr))) {
      shift <- plays_yr[n,]
      line <- strsplit(shift$playerIDs, split = "/") 
      for(person in line) {
        linemates <- c(linemates, person)
      }
    }
    #common_lines <- sort(table(linemates),decreasing=TRUE)[1:2]
    common_lines <- names(sort(summary(as.factor(linemates)), decreasing=T)[1:2])
    #the_lines <- names(common_lines)
    
    line1 <- common_lines[1]
    line2 <- common_lines[2]
    print(line1)
    print(line2)
    new_row$line_1 <- line1
    new_row$line_2 <- line2
    print(new_row)
    #colnames(new_row) <- new_play_colnames
    print(new_row)
    #print(new_row$linemate1)
    ret_df <- rbind(ret_df, new_row)
  }
  
  return(ret_df)
}

compile_team_abbrevs <- function(pbp, tm1, tm2, yr) {
  retVal <- c()
  for(r in c(1:nrow(pbp))) {
    added <- 0
    row <- pbp[r,]
    player <- row$playerIDs_col
    player_tm <- "000"
    if(player != "0") {
      if (grepl("/", player)) {
        parts <- strsplit(player, "/")[[1]]
        player <- parts[1]
      }
      yr2 <- as.numeric(yr)+1
      new_yr <- sprintf("%s%s", yr, yr2)
      poss_tms <- complete_roster[complete_roster$season == new_yr & complete_roster$id == player,]$team
  
      if(length(poss_tms) > 1) {
        for(t in c(1:length(poss_tms))) {
          if(poss_tms[t] == tm1 | poss_tms[t] == tm2) {
            player_tm <- poss_tms[t]
          }
        }
      } else if(length(poss_tms) == 0) {
        print("YOOOOOO")
        print(player)
        print(new_yr)
      } else {
        player_tm <- poss_tms[1]
      }
    }
    retVal <- c(retVal, player_tm)
  }
  return(retVal)
}

# takes in a dataframe of plays in the game. type should denote the type of play. 
get_winner <- function(game_df, tm1, tm2) {
  goals = game_df[game_df$type == "goal",]
  n_goals_tm1 = nrow(goals[goals$tmAbbrev == tm1,])
  if(n_goals_tm1 > (nrow(goals) - n_goals_tm1)) {
    return(tm1)
  } else {
    return(tm2)
  }
}

# Function to extract player IDs from the combined string
extract_player_ids <- function(player_ids_string) {
  ids <- unlist(strsplit(player_ids_string, "/"))
  # Remove empty strings
  
  return(ids[ids != ""])
  
  
}

# Helper function to process shot type and update counts
process_shot_type <- function(results, shooter_id, details) {
  if (!is.na(details)) {
    shot_type <- tolower(details)
    if (grepl("wrist", shot_type)) {
      results[[shooter_id]]$Wrist <- results[[shooter_id]]$Wrist + 1
    } else if (grepl("slap", shot_type)) {
      results[[shooter_id]]$Slap <- results[[shooter_id]]$Slap + 1
    } else if (grepl("snap", shot_type)) {
      results[[shooter_id]]$Snap <- results[[shooter_id]]$Snap + 1
    } else if (grepl("tip", shot_type)) {
      results[[shooter_id]]$Tip <- results[[shooter_id]]$Tip + 1
    } else if (grepl("deflect", shot_type)) {
      results[[shooter_id]]$Deflect <- results[[shooter_id]]$Deflect + 1
    }
  }
  return(results)
}

# Function to process play-by-play data and aggregate shot statistics
process_shot_stats <- function(play_by_play_df, stats_df, Szn) {
  # Validate Szn input
  if (missing(Szn)) {
    stop("Szn parameter is required")
  }
  
  # Create a list to store results, initialized with existing stats
  results <- list()
  
  # Initialize results with existing stats from stats_df
    #here
  
  # Process each row in the play-by-play data
  for (i in 1:nrow(play_by_play_df)) {
    row <- play_by_play_df[i, ]
    
    # Extract player IDs
    if (!is.na(row$playerIDs) && row$playerIDs != "") {
      player_ids <- extract_player_ids(row$playerIDs)
      
      # Get the shooting player (first ID in the list)
      shooter_id <- player_ids[1]
      
      # Initialize player stats if not exists
      if (!shooter_id %in% names(results)) {
        roster_info <- complete_roster[complete_roster$season == Szn & complete_roster$id == shooter_id,]
        results[[shooter_id]] <- list(
          PlayerId = shooter_id,
          Szn = Szn,
          tmAbbrev = row$tmAbbrev,
          Pos = roster_info$positionCode[1],
          shootsCatches = roster_info$shootsCatches[1],
          SOG = 0,
          Wrist = 0,
          Slap = 0,
          Snap = 0,
          Tip = 0,
          Deflect = 0,
          Missed = 0,
          Blocked = 0,
          Goals = 0, 
          Hits = 0
        )
      }
      
      # Process based on event type
      if (row$type == "shot-on-goal" || row$type == "goal") {
        # Increment SOG for both shots on goal and goals
        results[[shooter_id]]$SOG <- results[[shooter_id]]$SOG + 1
        
        # If it's a goal, increment the goals counter
        if (row$type == "goal") {
          results[[shooter_id]]$Goals <- results[[shooter_id]]$Goals + 1
        }
        
        # Process shot type for both shots and goals
        results <- process_shot_type(results, shooter_id, row$details)
        
      } else if (row$type == "blocked-shot") {
        results[[shooter_id]]$Blocked <- results[[shooter_id]]$Blocked + 1
        
      } else if (row$type == "missed-shot") {
        results[[shooter_id]]$Missed <- results[[shooter_id]]$Missed + 1
      } else if(row$type == "hit") {
        results[[shooter_id]]$Hits <- results[[shooter_id]]$Hits + 1
      }
    }
  }
  
  # Convert results list to data frame
  results_df <- do.call(rbind, lapply(results, function(x) {
    data.frame(
      PlayerId = x$PlayerId,
      Szn = x$Szn,
      tmAbbrev = x$tmAbbrev,
      Pos = x$Pos,
      shootsCatches = x$shootsCatches,
      SOG = x$SOG,
      Wrist = x$Wrist,
      Slap = x$Slap,
      Snap = x$Snap,
      Tip = x$Tip,
      Deflect = x$Deflect,
      Missed = x$Missed,
      Blocked = x$Blocked,
      Goals = x$Goals,
      Hits = x$Hits
    )
  }))
  
  big_player_df <- rbind(stats_df, results_df)
  big_player_df <- big_player_df %>% group_by(Szn, tmAbbrev, PlayerId, Pos, shootsCatches) %>%
    summarise(across(c(SOG, Wrist, Slap, Snap, Tip, Deflect, Missed, Blocked, Goals, Hits), sum))
  return(big_player_df)
}

get_game_counts <- function(big_df, df, year, tm1, tm2) {
  #print(tm1)
  #print(tm2)
  #print(year)


  empty_row <- rep(0, length(team_info_colnames))
  row_to_modify1 <- data.frame(matrix(nrow = 0, ncol = length(team_info_colnames)))
  row_to_modify1 <- rbind(row_to_modify1, empty_row)
  colnames(row_to_modify1) <- team_info_colnames
  
  row_to_modify2 <- data.frame(matrix(nrow = 0, ncol = length(team_info_colnames)))
  row_to_modify2 <- rbind(row_to_modify2, empty_row)
  colnames(row_to_modify2) <- team_info_colnames
  
  
  row_to_modify1$Szn <- year
  row_to_modify2$Szn <- year
  
  winner <- get_winner(df, tm1, tm2)
  if(winner == tm1) {
    row_to_modify1$W <- 1
    row_to_modify2$L <- 1
  } else {
    row_to_modify2$W <- 1
    row_to_modify1$L <- 1
  }
  
  row_to_modify1$tmAbbrev <- tm1
  row_to_modify2$tmAbbrev <- tm2

  
  sogs <- df[df$type == "shot-on-goal",]
  sog_1 <- sogs[sogs$tmAbbrev == tm1,] 
  sog_2 <- sogs[sogs$tmAbbrev == tm2, ]

  
  row_to_modify1$SOG = row_to_modify1$SOG + nrow(sog_1)
  row_to_modify2$SOG = row_to_modify2$SOG + nrow(sog_2)
  
  goals <- df[df$type == "goal", ]
  row_to_modify1$Goals = row_to_modify1$Goals + nrow(goals[goals$tmAbbrev == tm1,])
  row_to_modify2$Goals = row_to_modify2$Goals + nrow(goals[goals$tmAbbrev == tm2,])
  
  # get wrist shots from all players
  wrist_1 = sog_1[sog_1$details == "wrist",]
  wrist_2 = sog_2[sog_2$details == "wrist",]
  row_to_modify1$Wrist = row_to_modify1$Wrist + nrow(wrist_1)
  row_to_modify2$Wrist = row_to_modify2$Wrist + nrow(wrist_2)
  
  
  # there is also snap but i'm not sure of the difference...
  slap_1 = sog_1[sog_1$details == "slap",] 
  slap_2 = sog_2[sog_2$details == "slap",]
  row_to_modify1$Slap = row_to_modify1$Slap + nrow(slap_1)
  row_to_modify2$Slap = row_to_modify2$Slap + nrow(slap_2)
  
  snap_1 = sog_1[sog_1$details == "snap",] 
  snap_2 = sog_2[sog_2$details == "snap",]
  row_to_modify1$Snap = row_to_modify1$Snap + nrow(snap_1)
  row_to_modify2$Snap = row_to_modify2$Snap + nrow(snap_2)
  
  # get tip-ins for all players
  tip_1 = sog_1[sog_1$details == "tip-in",] 
  tip_2 = sog_2[sog_2$details == "tip-in",]
  row_to_modify1$Tip = row_to_modify1$Tip + nrow(tip_1)
  row_to_modify2$Tip = row_to_modify2$Tip + nrow(tip_2)
  
  missed_shots = df[df$type == "missed-shot",]
  missed_1 = missed_shots[missed_shots$tmAbbrev == tm1,]
  missed_2 = missed_shots[missed_shots$tmAbbrev == tm2,]
  row_to_modify1$Missed = row_to_modify1$Missed + nrow(missed_1)
  row_to_modify2$Missed = row_to_modify2$Missed + nrow(missed_2)
  
  blocked_shots = df[df$type == "blocked-shot",]
  blocked_1 = blocked_shots[blocked_shots$tmAbbrev == tm1,]
  blocked_2 = blocked_shots[blocked_shots$tmAbbrev == tm2,]
  row_to_modify1$Blocked = row_to_modify1$Blocked + nrow(blocked_1)
  row_to_modify2$Blocked = row_to_modify2$Blocked + nrow(blocked_2)
  
  big_df <- rbind(big_df, row_to_modify2)
  big_df <- rbind(big_df, row_to_modify1)
  return(big_df)
}


# will add the info in df to big_df and return it
add_info <- function(big_df, df, year, tm1, tm2) {
  
  curr_year = big_team_df[big_team_df$Szn == year,] 
  curr_player_df <- get_player_df(df, year)
  year_and_tm1 <- big_team_df[big_team_df$Szn == year & big_team_df$tmAbbrev == tm1,]
  year_and_tm2 <- big_team_df[big_team_df$Szn == year & big_team_df$tmAbbrev == tm2,]
  
  found1 <- FALSE
  found2 <- FALSE
  if(nrow(curr_year) == 0) {
    #print("here1")
    empty_row <- rep(0, length(team_info_colnames))
    row_to_modify1 <- data.frame(matrix(nrow = 0, ncol = length(team_info_colnames)))
    row_to_modify1 <- rbind(row_to_modify1, empty_row)
    #print(row_to_modify1)
    colnames(row_to_modify1) <- team_info_colnames
    
    row_to_modify2 <- data.frame(matrix(nrow = 0, ncol = length(team_info_colnames)))
    row_to_modify2 <- rbind(row_to_modify2, empty_row)
    colnames(row_to_modify2) <- team_info_colnames
    
  } else {
    #print("here2")
    empty_row <- rep(0, length(team_info_colnames))
    row_to_modify1 <- data.frame(matrix(nrow = 0, ncol = length(team_info_colnames)))
    row_to_modify1 <- rbind(row_to_modify1, empty_row)
    #print(row_to_modify1)
    colnames(row_to_modify1) <- team_info_colnames
    #print("here3")
    row_to_modify2 <- data.frame(matrix(nrow = 0, ncol = length(team_info_colnames)))
    row_to_modify2 <- rbind(row_to_modify2, empty_row)
    colnames(row_to_modify2) <- team_info_colnames
    #print("here4")
    
    if(tm1 %in% curr_year$tmAbbrev) {
      print(sprintf("found tm1 %s", tm1))
      row_to_modify1 <- curr_year[curr_year$tmAbbrev == tm1,]
      found1 <- TRUE
    }
    
    #row_to_modify2 <- data.frame(matrix(nrow = 0, ncol = length(team_info_colnames)))
    if(tm2 %in% curr_year$tmAbbrev) {
      found2 <- TRUE
      print(sprintf("found tm2 %s", tm2))
      row_to_modify2 <- curr_year[curr_year$tmAbbrev == tm2,]
    }
  }
  
  winner <- get_winner(df, tm1, tm2)
  
  #print("here")
  print(winner)
  #print(row_to_modify1)
  #print(row_to_modify2)
  
  if(winner == tm1) {
    #print("here winner")
    row_to_modify1$W <- row_to_modify1$W + 1
    row_to_modify2$L <- row_to_modify2$L + 1
  } else {
    row_to_modify2$W <- row_to_modify2$W + 1
    row_to_modify1$L <- row_to_modify1$L + 1
  }
  
  #print(year)
  
  row_to_modify1$Szn = year
  row_to_modify2$Szn = year
  
  row_to_modify1$tmAbbrev <- tm1
  row_to_modify2$tmAbbrev <- tm2
  
  sogs <- df[df$type == "shot-on-goal",]
  sog_1 <- sogs[sogs$tmAbbrev == tm1,] 
  sog_2 <- sogs[sogs$tmAbbrev == tm2, ]
  print(unique(sog_1))
  print(unique(sog_2))
  
  row_to_modify1$SOG = row_to_modify1$SOG + nrow(sog_1)
  row_to_modify2$SOG = row_to_modify2$SOG + nrow(sog_2)
  
  goals <- df[df$type == "goal", ]
  row_to_modify1$Goals = row_to_modify1$Goals + nrow(goals[goals$tmAbbrev == tm1,])
  row_to_modify2$Goals = row_to_modify2$Goals + nrow(goals[goals$tmAbbrev == tm2,])
  
  # get wrist shots from all players
  wrist_1 = sog_1[sog_1$details == "wrist",]
  wrist_2 = sog_2[sog_2$details == "wrist",]
  row_to_modify1$Wrist = row_to_modify1$Wrist + nrow(wrist_1)
  row_to_modify2$Wrist = row_to_modify2$Wrist + nrow(wrist_2)
  
  
  # there is also snap but i'm not sure of the difference...
  slap_1 = sog_1[sog_1$details == "slap",] 
  slap_2 = sog_2[sog_2$details == "slap",]
  row_to_modify1$Slap = row_to_modify1$Slap + nrow(slap_1)
  row_to_modify2$Slap = row_to_modify2$Slap + nrow(slap_2)
  
  # get tip-ins for all players
  tip_1 = sog_1[sog_1$details == "tip-in",] 
  tip_2 = sog_2[sog_2$details == "tip-in",]
  row_to_modify1$Tip = row_to_modify1$Tip + nrow(tip_1)
  row_to_modify2$Tip = row_to_modify2$Tip + nrow(tip_2)
  
  missed_shots = df[df$type == "missed-shot",]
  missed_1 = missed_shots[missed_shots$tmAbbrev == tm1,]
  missed_2 = missed_shots[missed_shots$tmAbbrev == tm2,]
  row_to_modify1$Missed = row_to_modify1$Missed + nrow(missed_1)
  row_to_modify2$Missed = row_to_modify2$Missed + nrow(missed_2)
  
  blocked_shots = df[df$type == "blocked-shot",]
  blocked_1 = blocked_shots[blocked_shots$tmAbbrev == tm1,]
  blocked_2 = blocked_shots[blocked_shots$tmAbbrev == tm2,]
  row_to_modify1$Blocked = row_to_modify1$Blocked + nrow(blocked_1)
  row_to_modify2$Blocked = row_to_modify2$Blocked + nrow(blocked_2)
  
  if(!found1) {
    print("found1")
    big_df <- rbind(big_df, row_to_modify1)
  }
  if(!found2) {
    print("found2")
    big_df <- rbind(big_df, row_to_modify2)
  }
  return(big_df)
  #print(big_team_df)
}
