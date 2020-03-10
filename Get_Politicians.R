options(useFancyQuotes=FALSE)
#For Windows authentication to SQL Server use the line below, for SQL Server authentication use SQL_Server_Connection_String="Driver=SQL Server;Server=(local);Database=Retweet;Uid=YourUserName;Pwd=YourPassword"
SQL_Server_Connection_String="Driver=SQL Server;Server=(local);Database=Politicians_Twitter_Followers;Trusted_Connection=true"
DataConnection<-RevoScaleR::RxSqlServerData(connectionString=SQL_Server_Connection_String,table='[Following]')
RevoScaleR::rxOpen(DataConnection,mode="w")

source(paste(getSrcDirectory(function(dummy) {dummy}), "./Twitter_Account_List.r", sep=""))  #Change codes in this file to map to your own Twitter application keys. The crazy stuff at the beginning of this line is a hack to get the current directory of the currently executing script.
Twitter_Token <- httr::Token1.0$new(endpoint=NULL,
                                    params=list(as_header=TRUE),
                                    app=httr::oauth_app(appname="twitter", key=Twitter_Account_List$ConsumerKey[1], secret=Twitter_Account_List$ConsumerSecret[1]),
                                    credentials=list(oauth_token=Twitter_Account_List$AccessToken[1],oauth_token_secret=Twitter_Account_List$AccessTokenSecret[1])
                                    )
data=httr::content(httr::GET("https://api.twitter.com/1.1/users/lookup.json",
                             query=list(screen_name="SenSanders,AndrewYang,BernieSanders,MikeBloomberg,SenWarren,EWarren,PeteButtigieg,SenAmyKlobuchar,AmyKlobuchar,JoeBiden,TulsiGabbard,TulsiPress,TomSteyer,POTUS,realDonaldTrump"),
                             httr::config(token=Twitter_Token)
                            ))
for (i in 1:length(data)) {
  	RevoScaleR::rxExecuteSQLDDL(DataConnection,sSQLString=paste("Exec Insert_Politician ",paste(sQuote(data[[i]]$id_str),sQuote(data[[i]]$screen_name),sQuote(data[[i]]$name),sQuote(gsub("'","''",data[[i]]$description)),as.character(data[[i]]$followers_count),as.character(data[[i]]$statuses_count),sQuote(data[[i]]$created_at),ifelse(data[[i]]$verified,1,0),sep=","),sep=""))
}
RevoScaleR::rxClose(DataConnection)
