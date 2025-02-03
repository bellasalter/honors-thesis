library(foreach)
source("data_processing.R")
library(dplyr)
library(tidyr)
library(hash)

large_df_rows <- 5000
team_info_colnames <- c("Szn", "TmAbbrev", "W", "L", "SOG", "Wrist", "Slap", "Snap", "Tip", "Deflect", "Missed", "Blocked", "Goals")
play_info_colnames <- c("PlayerId","Pos", "Szn", "TmAbbrev", "SOG", "Wrist", "Slap", "Snap", "Tip", "Deflect", "Missed", "Blocked", "Goals")
complete_roster <- read_csv("data_collection/player_data/all_seasons_rosters.csv")
res_out_colnames<- c("Szn", "GameID", "TOI", "TmAbbrev", "NumPlayers", "PlayerIds", "SitCode", "TypeCode", "ZoneCode", "Details")
num_cores <- parallel::detectCores() - 1

do_primary_collection <- function() {
  game_ids <- get_game_ids("data_collection/shift_data")

  #schedule_info <- read_csv("./nhl_schedule.csv")
  game_ids_df<- data.frame(game_ids)
  game_ids_df$yr <- substring(game_ids_df$game_ids, 1, 4)
  
  yrs <- c("2011", "2012", "2013", "2014", "2015") 
  for(yr in yrs) {
    curr_yr_ids <- game_ids_df[game_ids_df$yr == yr,]$game_ids
    print(sprintf("Retrieved %s game ids for year %s --------------------------------", length(curr_yr_ids), yr))
    res <- get_all_info_parallel(yr, curr_yr_ids, debug=FALSE)
  }
}

unregister_dopar <- function() {
  env <- foreach:::.foreachGlobals
  rm(list=ls(name=env), pos=env)
}


get_all_info_parallel <- function(year, fns, debug=FALSE) {
  start_time <- Sys.time()
  
  # guess number of rows, should speed up computation
  big_team_df <- data.frame(matrix(nrow = large_df_rows, ncol = length(team_info_colnames)))
  big_player_df <- data.frame(matrix(nrow = large_df_rows, ncol = length(play_info_colnames)))
  colnames(big_team_df) <- team_info_colnames
  colnames(big_player_df) <- play_info_colnames
  
  roster_dt <- as.data.table(complete_roster)
  setkey(roster_dt, season, id)
  
  cl <- makeCluster(num_cores)
  clusterExport(cl, c("debug"))
  on.exit(stopCluster(cl), add = TRUE)
  registerDoParallel(cl)
  print(sprintf("Starting data collection clusters on on %s cores....", num_cores))
  
  fns <- fns[!is.na(fns) & nzchar(fns)]

  results <- foreach(i=1:length(fns), 
                     .packages = c('data.table', 'readr', 'dplyr', 'hash', 'tidyr'),
                     .combine = 'rbind') %dopar% {
      mini_row_colnames <- c("TOI", "NumPlayers", "PlayerIds", "SitCode", "TypeCode", "ZoneCode", "Details")
      process_json <- function(json_data, requested_attr) {
        
        if(identical(grep(requested_attr, json_data),integer(0))) {
          return("")
        }
        start_ind <- gregexpr(pattern =requested_attr,json_data)[[1]][1] + nchar(requested_attr)
        
        end_json <- gregexpr(pattern ="}",json_data)[[1]][1]
        rest_of_json <- substring(json_data, start_ind, nchar(json_data))
        end_attr <- gregexpr(pattern=", ",rest_of_json)[[1]][1]
        to_ret = -1
        if(!(is.na(end_attr)) & end_attr != -1) {
          to_ret = substring(json_data, start_ind + 3, end_attr+start_ind - 2)
        } else {
          to_ret = substring(json_data, start_ind + 3, end_json+start_ind-2)
          to_ret = gsub("}", "", to_ret)
        }
        if(grepl("\'", to_ret, fixed=TRUE)) {
          to_ret = gsub("'", '', to_ret)
        }
        
        return(to_ret)
      }
      
      create_new_row <- function(deets, toi, sitcode, typecode, zonecode, player_ids) {
        two_rows <- FALSE
        mini_type <- substring(typecode,1,2)
        details <- "000"
        if(mini_type == "fa") { #faceoff
          num_players <- 1
          player_ids <- process_json(deets, "winningPlayerId")
          
        } else if(mini_type == "gi" || mini_type == "ta") { #giveaway or takeaway
          num_players <- 1
          player_ids <- process_json(deets, "playerId")
          
        } else if(mini_type == "hi") { #hit
          num_players <- 2
          player_ids <- process_json(deets, "hittingPlayerId")
          new_row_2 <- create_new_row(deets, toi, sitcode, "received-hit", zonecode, process_json(deets,
                                                                                                  "hitteePlayerId"))
          two_rows <- TRUE
        } else if(mini_type == "pe") { #penalty
          num_players <- 2
          player_ids <- process_json(deets, "committedByPlayerId")
          
          details <- process_json(deets, "descKey")
          
          drawing_player <- process_json(deets, "drawnByPlayerId")
          if(drawing_player != "") {
            new_row_2 <- create_new_row(deets, toi, sitcode, "drawn-penalty", zonecode, drawing_player)
            two_rows <- TRUE
          }
          
        } else if(mini_type == "sh" || mini_type == "mi") { #sog or missed
          num_players <- 1
          player_ids <- process_json(deets, "shootingPlayerId")
          details <- process_json(deets, "shotType")
          
        } else if(mini_type == "bl") { #blocked
          num_players <- 2 # blockingPlayerId
          player_ids <- process_json(deets, "blockingPlayerId")
          
        } else if(mini_type == "go") { #goal
          num_players <- 1 #default??
          player_ids <- process_json(deets, "scoringPlayerId")
          first_assist <- process_json(deets,"assist1PlayerId")
          if(first_assist != "") {
            
            new_row_2 <- create_new_row(deets, toi, sitcode, "assist", zonecode, first_assist)
            two_rows <- TRUE
            
            second_assist <- process_json(deets, "assist2PlayerId")
            if(second_assist != "") {
              new_row_3 <- create_new_row(deets, toi, sitcode, "assist", zonecode, second_assist)
              new_row_2 <- rbind(new_row_2, new_row_3)
            }

          }
          details <- process_json(deets, "shotType")
          
        } else if(mini_type == "as" || mini_type == "dr" || mini_type == "re") { #assist, drawn penalty, received hit
          num_players <- 1
        }
        else {
          num_players <- NULL
        }
        
        # if its none of the ones we care about, keep going
        if(is.null(num_players)) {
          return()
        }
        
        new_row <- data.frame(matrix(c(toi, num_players, player_ids, sitcode, typecode, zonecode, details), nrow=1))
        
        if(two_rows) {
          
          new_row <- rbind(new_row, new_row_2)
        }

        return(new_row)
      }
      
      process_plays_dt <- function(play_data, player_info, year) {
        new_plays<- data.frame(matrix(nrow = nrow(play_data), ncol = length(mini_row_colnames)))
        colnames(new_plays) <- mini_row_colnames
        
          for(i in 1:nrow(play_data)) {
            curr_row <- play_data[i,]
            type <- curr_row$typeDescKey
            deets <- curr_row$details
            toi <- curr_row$TOI
            sitcode <- curr_row$situationCode
            typecode <- curr_row$typeDescKey
            zonecode <- process_json(deets, "zoneCode")
            
            new_row <- create_new_row(deets, toi, sitcode, typecode, zonecode, player_ids)
            if(is.null(new_row)) {
              next
            }
            colnames(new_row) <- mini_row_colnames
            new_plays <- rbind(new_plays, new_row)
          }
        
        return(new_plays)
      }
      create_player_team_hash <- function(df) {
        player_teams <- df %>%
          separate_rows(PlayerIds, sep = "/") %>%
          mutate(PlayerIds = trimws(PlayerIds)) %>%
          filter(PlayerIds != "") %>%
          group_by(PlayerIds) %>%
          slice_tail(n = 1) %>%
          ungroup() %>%
          select(PlayerIds, TmAbbrev)

        h <- hash()
        for(i in seq_len(nrow(player_teams))) {
          h[[player_teams$PlayerIds[i]]] <- player_teams$TmAbbrev[i]
        }
        
        return(h)
      }
      

      game <- fns[i]
                       
      year_substr <- substring(game, 1, 4)
      
      shift_path <- sprintf("data_collection/shift_data/%s/%s", year_substr, game)
      play_path <- sprintf("data_collection/play_data/%s/%s", year_substr, game)
      
      if(!file.exists(shift_path) || !file.exists(play_path)) {
        return(NULL)
      }
  
      # get shift data
      shift_data <- fread(shift_path, check.names=TRUE)
      
      shift_data <- shift_data[,':='(eventDescription = NULL, eventDetails = NULL, eventNumber=NULL, hexValue=NULL, detailCode = NULL)]   
      
      setnames(shift_data, 
               old = colnames(shift_data),
               new = c("GameId", "TmAbbrev", "firstName", "lastName", "period",
                       "startTime", "endTime", "shiftNumber", "TypeCode", "teamId",
                       "playerId", "teamName", "id", "opTeamAbbrev", "duration"))
      
      shift_data[, startTimeI := sprintf("P%s-%s", period, startTime)]
      shift_data[, playerName := sprintf("%s_%s", firstName, lastName)]
      shift_data[, ':=' (firstName = NULL, lastName = NULL)]
  
      
      player_info <- unique(shift_data[, .(playerId, playerName, TmAbbrev = first(TmAbbrev))])
      
      player_info <- merge(player_info,
                           roster_dt[season == substring(game, 1, 4), 
                                              .(id, positionCode, shootsCatches)],
                           by.x = "playerId",
                           by.y = "id",
                           all.x = TRUE)
      
      
      shift_summary <- shift_data[, .(
        NumPlayers = .N,
        PlayerIds = paste(playerId, collapse = "/")
      ), by = .(startTimeI, TmAbbrev)]
      shift_summary[, TOI := ((as.numeric(substring(startTimeI, 2, 2))-1)* 1200 + (60*as.numeric(substring(startTimeI, 4,5))) + as.numeric(substring(startTimeI, 7,8)))]
      shift_summary[, ":=" (startTimeI=NULL)]
      
      # get play data, don't want shootouts
      play_data <- fread(play_path, check.names=TRUE)
      
      play_data[, `:=`(
        TOI = (as.numeric(substring(periodDescriptor, 12, 12))-1) * 1200 + as.numeric(substring(timeInPeriod,1,2))*60 + as.numeric(substring(timeInPeriod,4,5)),
        periodDescriptor = NULL,
        timeInPeriod = NULL,
        timeRemaining = NULL,
        pptReplayUrl = NULL,
        sortOrder = NULL,
        typeCode = NULL,
        homeTeamDefendingSide = NULL,
        eventId = NULL
      )]
    
      
      if("shootout-complete" %in% play_data$type) {
        return(NULL)
      }
      
      shift_summary$TypeCode <- rep("shift-change", nrow(shift_summary))
      processed_plays <- process_plays_dt(play_data, player_info, year)

      # dealing with TmAbbrev stuff
      player_team_hash <- create_player_team_hash(shift_summary)
      processed_plays$prinPlayer <- substring(processed_plays$PlayerIds,1, 7)
      processed_plays <- processed_plays %>%
        mutate(TmAbbrev = sapply(prinPlayer, function(x) {
          tryCatch({
            val <- player_team_hash[[x]]
            if(is.null(val) || length(val) == 0) NA_character_
            else as.character(val)
          }, error = function(e) NA_character_)
        }))
      
      #processed_plays <- transform_events(processed_plays)
      
      final_dt <- rbindlist(list(shift_summary, processed_plays), fill = TRUE)
      final_dt[, `:=`(
        yr = substring(game, 1, 4),
        game_id = substr(game, 1, nchar(game)-4)
      )]
      
      return(final_dt)
 }
  
  unregister_dopar()
  
  print(sprintf("Calculating aggregate statistics...."))

  player_agg <- agg_stats(results, year)
  #team_agg <- agg_stats_team(player_agg)
  
  file.create(sprintf("data_processing/%s_player_agg_df.csv", year))
  #file.create(sprintf("./%s_df/team_agg_df.csv", year))
  file.create(sprintf("./%s_everything_df.csv", year))
  
  write.csv(player_agg, file=sprintf("data_processing/%s_player_agg_df.csv", year))
  #write.csv(team_agg, file=sprintf("./%s_df/team_agg_df.csv", year))
  write.csv(results, file=sprintf("./%s_everything_df.csv", year))
  
  if(debug) {
    print(results)
  }
  
  end_time <- Sys.time()
  print(sprintf("Running year %s complete. Runtime: %s seconds.", year, (end_time-start_time)))
  return(results)
}

# gets the aggregate statistics for each player
agg_stats <- function(results, year) {
  gen_plays <- results[results$TypeCode != "shift-change",]
  #ret_df <- data.frame(matrix(nrow = large_df_rows, ncol = length(player_info_colnames)))
  
  cl <- makeCluster(num_cores)
  clusterExport(cl, c("debug"))
  on.exit(stopCluster(cl), add = TRUE)
  registerDoParallel(cl)
  
  list_attrs <- c("PlayerId", "yr", "TmAbbrev", "G", "A", "Hits", "Blocked", "Missed", "SOG", "Wrist","Snap","Tip","Back","Slap", "Deflected")#, "WristSP", "SnapSP", "TipSP", "BackSP", "SlapSP")
  
  players <- unique(results$PlayerIds)
  
  ret_vals <- foreach(i=1:length(players), 
                     .packages = c('data.table', 'readr', 'dplyr', 'hash', 'tidyr'),
                     .combine = 'rbind') %dopar% {
     player_id <- players[i]       
     rel_data <- results[results$PlayerIds == player_id, ]
     curr_ret <- data.frame(matrix(nrow = 1, ncol = length(list_attrs)))
     curr_ret$PlayerId <- player_id
     curr_ret$yr <- year
     curr_ret$TmAbbrev <- rel_data[1,TmAbbrev]
     curr_ret$G <- nrow(rel_data[rel_data$TypeCode == "goal",])
     curr_ret$A <- nrow(rel_data[rel_data$TypeCode == "assist",])
     curr_ret$Hits <- nrow(rel_data[rel_data$TypeCode == "hit",])
     curr_ret$Blocked <- nrow(rel_data[rel_data$TypeCode == "blocked-shot",])
     curr_ret$Missed <- nrow(rel_data[rel_data$TypeCode == "missed-shot",])
     
     sogs <- rel_data[rel_data$TypeCode == "shot-on-goal",]
     curr_ret$SOG <- nrow(sogs)
     curr_ret$Wrist <- nrow(sogs[sogs$details == "wrist",])
     curr_ret$Snap <- nrow(sogs[sogs$details == "snap",])
     curr_ret$Tip <- nrow(sogs[sogs$details == "tip",])
     curr_ret$Back <- nrow(sogs[sogs$details == "backhand",])
     curr_ret$Slap <- nrow(sogs[sogs$details == "slap",])
     curr_ret$Deflected <- nrow(sogs[sogs$details == "deflected",])
     
     return(curr_ret)
  }
  
  unregister_dopar()
  return(ret_vals)
}

