options(useFancyQuotes=FALSE)
#options(warn=2,error=recover)

source(paste(getSrcDirectory(function(dummy) {dummy}), "./Twitter_Account_List.r", sep=""))  #Change codes in this file to map to your own Twitter application keys. The crazy stuff at the beginning of this line is a hack to get the current directory of the currently executing script.
source(paste(getSrcDirectory(function(dummy) {dummy}), "./Find_or_Wait_For_Available_Twitter_Account.r", sep=""))  #Source code to shared function that does what it's name implies

SQL_Server_Connection_String="Driver=SQL Server;Server=(local);Database=Politicians_Twitter_Followers;Trusted_Connection=true"
DataConnection<-RevoScaleR::RxSqlServerData(connectionString=SQL_Server_Connection_String,table='[Tweet]')
RevoScaleR::rxOpen(DataConnection,mode="w")

Rate_Requests_Available=0
Tweet_Requests_Available=0
Twitter_Account=0
Twitter_Token=NULL
Start_Time=Sys.time()

SQL="Select Top 500 Cast(Id As VarChar(19)) As Id,Case When Screen_Name Is Null Then 1 Else 0 End As Need_Profile From [User] TableSample (10000 rows) With (nolock) Where Tweets>0 And Inactive Is Null Order By NewId()"

while (difftime(Sys.time(),Start_Time,units="mins")<15.0) {
  Users=RevoScaleR::rxImport(RevoScaleR::RxSqlServerData(connectionString=SQL_Server_Connection_String,
                                                         sqlQuery=SQL,
                                                           reportProgress=0,verbose=0,rowsPerRead=500),reportProgress=0,verbose=0)
  if (nrow(Users)==0) break

  for (i in 1:nrow(Users)) {
    first_batch=TRUE
    User_Not_Done=TRUE
    while(User_Not_Done&&difftime(Sys.time(),Start_Time,units="mins")<15.0) {

      Twitter_Account_Usage_Info=Find_or_Wait_For_Available_Twitter_Account(Twitter_Account,Twitter_Token,Rate_Requests_Available,Tweet_Requests_Available,"statuses")
      Twitter_Account=Twitter_Account_Usage_Info[[1]]
      Rate_Requests_Available=Twitter_Account_Usage_Info[[2]]
      Tweet_Requests_Available=Twitter_Account_Usage_Info[[3]]
      Twitter_Token=Twitter_Account_Usage_Info[[4]]

      if (first_batch) {
        if (Users[i,2]==0)
          Get_Request=httr::RETRY("GET","https://api.twitter.com/1.1/statuses/user_timeline.json",
                                query=list(user_id = Users[i,1], since_id="550441286000000000",count=200,trim_user='true',tweet_mode='extended',exclude_replies='true',include_rts='false',stringify_ids=TRUE),
                                httr::config(token=Twitter_Token),terminate_on=c(401,200))
        else
          Get_Request=httr::RETRY("GET","https://api.twitter.com/1.1/statuses/user_timeline.json",
                                query=list(user_id = Users[i,1], since_id="550441286000000000",count=200,tweet_mode='extended',exclude_replies='true',include_rts='false',stringify_ids=TRUE),
                                httr::config(token=Twitter_Token),terminate_on=c(401,200))

        first_row=1
      } else {
        if (Users[i,2]==0) {
          Get_Request=httr::RETRY("GET","https://api.twitter.com/1.1/statuses/user_timeline.json",
                              query=list(user_id = Users[i,1], since_id="550441286000000000",max_id=max_id,count=200,trim_user='true',tweet_mode='extended',exclude_replies='true',include_rts='false',stringify_ids=TRUE),
                              httr::config(token=Twitter_Token),terminate_on=c(401,200))
        } else {
          Get_Request=httr::RETRY("GET","https://api.twitter.com/1.1/statuses/user_timeline.json",
                              query=list(user_id = Users[i,1], since_id="550441286000000000",max_id=max_id,count=200,tweet_mode='extended',exclude_replies='true',include_rts='false',stringify_ids=TRUE),
                              httr::config(token=Twitter_Token),terminate_on=c(401,200))
        }
        first_row=2
      }
      Tweet_Requests_Available=Tweet_Requests_Available-1
      #Error about not seeing tweets?
      if (httr::http_error(Get_Request)) {
        if (httr::http_status(Get_Request)$message=="Client error: (403) Forbidden") {
          RevoScaleR::rxExecuteSQLDDL(DataConnection,sSQLString=paste("Exec Inactive_User ",Users[i,1],sep=""))
        }
        break
      }
      data=httr::content(Get_Request,encoding='UTF-8')
      rm(Get_Request)


      if (length(data)==first_row-1) {
        if (first_batch) {
          RevoScaleR::rxExecuteSQLDDL(DataConnection,sSQLString=paste("Exec Inactive_User ",Users[i,1],sep=""))
        }
        break
      }
      first_batch=FALSE
      j=first_row

      while (j<=length(data)) {
        if (j==1) {
          RevoScaleR::rxExecuteSQLDDL(DataConnection,sSQLString=paste("Exec Last_Tweeted_On ",sQuote(Users[i,1]),',',sQuote(data[[j]]$created_at),sep=""))
        }
        max_id=data[[j]]$id_str
        if (Users[i,2]==1) {
            RevoScaleR::rxExecuteSQLDDL(DataConnection,
                                sSQLString=paste("Exec Update_User ",paste(sQuote(Users[i,1]),sQuote(data[[j]]$user$screen_name),sQuote(gsub("'","''",data[[j]]$user$name)),sQuote(gsub("'","''",data[[j]]$user$description)),as.character(data[[j]]$user$followers_count),as.character(data[[j]]$user$friends_count),ifelse(is.null(data[[j]]$user$lang),'NULL',data[[j]]$user$lang),as.character(data[[j]]$user$statuses_count),sQuote(data[[j]]$user$created_at),ifelse(data[[j]]$user$verified,1,0),sep=","),sep="")
                                )
        }
        SQL=paste("Exec Insert_Tweet ",paste(sQuote(data[[j]]$id_str),sQuote(as.character(Users[i,1])),sQuote(gsub("'","''",data[[j]]$full_text)),sQuote(data[[j]]$created_at),'NULL',ifelse(is.null(data[[j]]$lang),'NULL',sQuote(data[[j]]$lang)),ifelse(is.null(data[[j]]$in_reply_to_status_id_str),'0','1'),as.character(data[[j]]$retweet_count),as.character(data[[j]]$favorite_count),
                                             ifelse(is.null(data[[j]]$in_reply_to_user_id_str),'NULL',sQuote(data[[j]]$in_reply_to_user_id_str)),
                                             ifelse(is.null(data[[j]]$in_reply_to_status_id_str),'NULL',sQuote(data[[j]]$in_reply_to_status_id_str)),
                                             ifelse(is.null(data[[j]]$quoted_status_id_str),'NULL',sQuote(data[[j]]$quoted_status_id_str)),
                                             ifelse(is.null(data[[j]]$quoted_status_id_str)||is.null(data[[j]]$quoted_status),'NULL',sQuote(data[[j]]$quoted_status$user$id_str)),
                                             sep=","),sep="")
        RevoScaleR::rxExecuteSQLDDL(DataConnection,sSQLString=SQL)
        if (is.null(data[[j]]$in_reply_to_status_id_str)&&!is.null(data[[j]]$entities)) {
          if (length(data[[j]]$entities$user_mentions)!=0) {
            for (k in 1:length(data[[j]]$entities$user_mentions)) {
              SQL2=paste("Exec Insert_Tweet_Mention ",paste(sQuote(data[[j]]$id_str),sQuote(data[[j]]$entities$user_mentions[[k]]$screen_name),sep=","),sep="")
              RevoScaleR::rxExecuteSQLDDL(DataConnection,sSQLString=SQL2)
            }
          }
        }
        j=j+1
      }

      rm(data)
      foo=gc(full=TRUE)
    }
  }
}