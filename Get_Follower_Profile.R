options(useFancyQuotes=FALSE)
options(warn=2,error=recover)

source(paste(getSrcDirectory(function(dummy) {dummy}), "./Twitter_Account_List.r", sep=""))  #Change codes in this file to map to your own Twitter application keys. The crazy stuff at the beginning of this line is a hack to get the current directory of the currently executing script.
source(paste(getSrcDirectory(function(dummy) {dummy}), "./Find_or_Wait_For_Available_Twitter_Account.r", sep=""))  #Source code to shared function that does what it's name implies
SQL_Server_Connection_String="Driver=SQL Server;Server=(local);Database=Politicians_Twitter_Followers;Trusted_Connection=true"
RevoScaleR::rxSetComputeContext(RevoScaleR::RxInSqlServer(connectionString=SQL_Server_Connection_String,shareDir=paste("C:\\AllShare\\",Sys.getenv("USERNAME"),sep=""),wait=TRUE,consoleOutput=FALSE))
DataConnection<-RevoScaleR::RxSqlServerData(connectionString=SQL_Server_Connection_String,table='[Following]')
RevoScaleR::rxOpen(DataConnection,mode="w")
#SQL="Select String_Agg(Id,',') As Follower_Ids From (Select Top 100 Id From [User] TableSample (10000 rows) with (nolock) Where Profile_Done = 0 Order By NewId()) Temp"
SQL="Select String_Agg(Id,',') As Follower_Ids From (Select Top 100 Id From [User] with (nolock) Where Profile_Done = 0) Temp"

Users=RevoScaleR::rxImport(RevoScaleR::RxSqlServerData(connectionString=SQL_Server_Connection_String,
                                                        sqlQuery=SQL,
                                                        reportProgress0=0,verbose=0),reportProgress=0,verbose=0)
Twitter_Account_Usage_Info=c(0,0,0,NULL)
Start_Time=Sys.time()
while (length(Users)>0&&difftime(Sys.time(),Start_Time,units="mins")<10.0) {
  Twitter_Account_Usage_Info=Find_or_Wait_For_Available_Twitter_Account(Twitter_Account_Usage_Info[[1]],Twitter_Account_Usage_Info[[4]],Twitter_Account_Usage_Info[[2]],Twitter_Account_Usage_Info[[3]],"users")


  Get_Request=httr::GET("https://api.twitter.com/1.1/users/lookup.json",
  	                         query=list(user_id = Users$Follower_Ids, stringify_ids=TRUE,count=100),
  	                         httr::config(token=Twitter_Account_Usage_Info[[4]],httr::timeout(5)))
  Twitter_Account_Usage_Info[[3]]=Twitter_Account_Usage_Info[[3]]-1
  if (!httr::http_error(Get_Request)) {
    data=httr::content(Get_Request,encoding='UTF-8')
    delete=FALSE
  } else {
    if(httr::http_status(Get_Request)$message=="Client error: (404) Not Found") {
      data=unlist(strsplit(Users$Follower_Ids,","))
      delete=TRUE
    } else next
  }
  for (i in 1:length(data)) {
    if (!delete) {
      RevoScaleR::rxExecuteSQLDDL(DataConnection,
                                  sSQLString=paste("Exec Update_User ",
                                                   paste(sQuote(data[[i]]$id_str),
                                                         sQuote(data[[i]]$screen_name),
                                                         ifelse(is.null(data[[i]]$name)|data[[i]]$name=='','Null',sQuote(gsub("'","''",data[[i]]$name))),
                                                         ifelse(is.null(data[[i]]$description)|data[[i]]$description=='','Null',sQuote(gsub("'","''",data[[i]]$description))),
                                                         as.character(data[[i]]$followers_count),
                                                         as.character(data[[i]]$friends_count),
                                                         ifelse(is.null(data[[i]]$lang)|data[[i]]$lang=='','NULL',data[[i]]$lang),
                                                         as.character(data[[i]]$statuses_count),
                                                         sQuote(data[[i]]$created_at),
                                                         ifelse(data[[i]]$verified,1,0),
                                                         ifelse(data[[i]]$geo_enabled,1,0),
                                                         ifelse(is.null(data[[i]]$location)|data[[i]]$location=='','Null',sQuote(gsub("'","''",data[[i]]$location))),
                                                         ifelse(is.null(data[[i]]$time_zone)|data[[i]]$time_zone=='','Null',sQuote(gsub("'","''",data[[i]]$time_zone$tzinfo_name))),
                                                         sep=","
                                                        ),
                                                   sep=""
                                                  )
                                 )
    } else {
      SQL=paste("Delete From [User2User] Where Followee=Cast(",sQuote(data[[i]]$id_str)," As BigInt);Delete From [User] Where Id=Cast(",sQuote(data[i])," As BigInt);",sep="")
      print(SQL)
      RevoScaleR::rxExecuteSQLDDL(DataConnection,sSQLString=SQL)
    }
  }
  Users=RevoScaleR::rxImport(RevoScaleR::RxSqlServerData(connectionString=SQL_Server_Connection_String,sqlQuery=SQL,reportProgress=0,verbose=0),reportProgress=0,verbose=0)
}
