Find_or_Wait_For_Available_Twitter_Account=function (Twitter_Account,Twitter_Token,Rate_Requests_Available,Requests_Available,Request_Type) {

  if ((Rate_Requests_Available<=4)|(Requests_Available<=3)) {
    # This program is necessary to deal with rate limiting in Twitter's API. You can only issue so many requests of a given type in a given time frame.
    # See https://developer.twitter.com/en/docs/basics/rate-limits and https://developer.twitter.com/en/docs/basics/rate-limiting for details
    
    # This program doesn't explicitly track all known requests so using small non-zero number immediately below SEEMS to account for those untracked requests
    
    #Find a different app or wait until Twitter requests are available
    Wait_Seconds=900/length(Twitter_Account_List)
    for (i in 1:(nrow(Twitter_Account_List))) {
      Twitter_Account=sample.int(nrow(Twitter_Account_List),size=1)
      cat("authorization code of app ",Twitter_Account,"\r\n",sep="")

      #Authenticate your Twitter account
      Twitter_Token = httr::Token1.0$new(endpoint=NULL,
                                        params=list(as_header=TRUE),
                                        app=httr::oauth_app("twitter",key=Twitter_Account_List$ConsumerKey[Twitter_Account],secret=Twitter_Account_List$ConsumerSecret[Twitter_Account]),
                                        credentials=list(oauth_token=Twitter_Account_List$AccessToken[Twitter_Account],oauth_token_secret=Twitter_Account_List$AccessTokenSecret[Twitter_Account])
                                        )
      # Get number of Twitter requests available during the current 15 minute time period
      Get_Request=httr::GET("https://api.twitter.com/1.1/application/rate_limit_status.json",query=list(resources=paste(Request_Type,"application",sep=",")),
                httr::config(token=Twitter_Token))

      if (httr::http_error(Get_Request)) next
      data=httr::content(Get_Request)
      if (Request_Type=="search") {
        Requests_Available=data$resources$search$`/search/tweets`$remaining
      } else if (Request_Type=="followers") {
        Requests_Available=data$resources$followers$`/followers/ids`$remaining
      } else if (Request_Type=="statuses") {
        Requests_Available=data$resources$statuses$`/statuses/retweets/:id`$remaining
      } else if (Request_Type=="friends") {
        Requests_Available=data$resources$friends$`/friends/ids`$remaining
      } else if (Request_Type=="users") {
        Requests_Available=data$resources$users$`/users/lookup`$remaining
      }

      Rate_Requests_Available=data$resources$application$`/application/rate_limit_status`$remaining

      #We have enough available requests so exit loop
      if (Requests_Available>3) {
        Wait_Seconds=0
        break
      }
    }
    if (Wait_Seconds>0) {
      Sys.sleep(Wait_Seconds)
    }
  }
  return(list(Twitter_Account,Rate_Requests_Available,Requests_Available,Twitter_Token))
}