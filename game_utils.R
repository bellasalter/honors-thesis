

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
