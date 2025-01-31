
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

get_game_ids <- function(folder_name) {
  game_ids <- c()
  for(year in c(list.files(path=folder_name))) {
    # we always have play-by-play, but not always shift_data. thus, get info abt shift and check for matching play_data
    new_path <- folder_name
    new_path <- paste(new_path, "/", sep="")
    new_path <- paste("shift_data/", year, sep="")
    new_path <- paste(new_path, "/", sep="")
    for(game in list.files(path=new_path)){
      game_ids <- c(game_ids, game)
    }
  }
  print(sprintf("Successfully retrieved %s game filenames.", length(game_ids)))
  return(game_ids)
}


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
