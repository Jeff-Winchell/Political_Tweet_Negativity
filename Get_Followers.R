options(warn=2,error=recover)
#options(useFancyQuotes=FALSE)

import_data=function(user,Cursor,Token,Available,Cnt,Cand) {
	data=httr::content(httr::GET("https://api.twitter.com/1.1/followers/ids.json",
					query=list(user_id=user,cursor=Cursor,stringify_ids=TRUE,count=5000),
					httr::config(token=Token)))
    if (length(data$ids)>0) {
        if (!dir.exists(Cand)) dir.create(Cand)
        if (!dir.exists(paste(Cand,"\\",as.character(ceiling(Cnt/1000000)),sep=""))) dir.create(paste(Cand,"\\",as.character(ceiling(Cnt/1000000)),sep=""))
            write.table(as.data.frame(unlist(data$ids)),paste(Cand,"\\",as.character(ceiling(Cnt/1000000)),"\\",as.character(ceiling(Cnt/5000)%%200),".csv",sep=""),row.names=FALSE,col.names=FALSE)
    }
	return(list(data$next_cursor_str,Available-1,Cnt+length(data$ids)))
}

Get_Followers=function(Followee=Null) {
    source(paste(getSrcDirectory(function(dummy) {dummy}), "./Twitter_Account_List.r", sep=""))
    source(paste(getSrcDirectory(function(dummy) {dummy}), "./Find_or_Wait_For_Available_Twitter_Account.r", sep=""))

	  SQL_Server_Connection_String="Driver=SQL Server;Server=(local);Database=Politicians_Twitter_Followers;Trusted_Connection=true"
    RevoScaleR::rxSetComputeContext(RevoScaleR::RxInSqlServer(connectionString=SQL_Server_Connection_String,shareDir=paste("C:\\AllShare\\",Sys.getenv("USERNAME"),sep=""),wait=TRUE,consoleOutput=FALSE))
    DataConnection<-RevoScaleR::RxSqlServerData(connectionString=SQL_Server_Connection_String,table='[Following]')
    RevoScaleR::rxOpen(DataConnection,mode="w")
    if (is.null(Followee)) {
        Users=RevoScaleR::rxImport(RevoScaleR::RxSqlServerData(connectionString=SQL_Server_Connection_String,sqlQuery="Select Id,Screen_Name From [User] (nolock) Where Politician=1 Order By Followers Desc",rowsPerRead=500))
    } else {
        Users=RevoScaleR::rxImport(RevoScaleR::RxSqlServerData(connectionString=SQL_Server_Connection_String,sqlQuery=paste("Select Id From [User] (nolock) Where Screen_Name = ",sQuote(Followee),sep=""),rowsPerRead=500))
    }

    #Initialize values that are changed in function Find_or_Wait_For_Available_Twitter_Account()
    Rate_Requests_Available=0
    Followers_Requests_Available=0
    Twitter_Account=0
    Twitter_Token=NULL
    for (i in 1:nrow(Users)) {
        print(ifelse(is.null(Followee),Users[i,2],Followee))
        Starting_Follower_Id_Of_Next_Page=-1 #Special Id which means start from the first page
        idcnt=0
        while (!is.null(Starting_Follower_Id_Of_Next_Page)){
        #If there are no more pages of ids, break out of loop
            if (Starting_Follower_Id_Of_Next_Page==0) break
            Twitter_Account_Usage_Info=Find_or_Wait_For_Available_Twitter_Account(Twitter_Account,Twitter_Token,Rate_Requests_Available,Followers_Requests_Available,"followers")
            Twitter_Account=Twitter_Account_Usage_Info[[1]]
            Rate_Requests_Available=Twitter_Account_Usage_Info[[2]]
            Followers_Requests_Available=Twitter_Account_Usage_Info[[3]]
            Twitter_Token=Twitter_Account_Usage_Info[[4]]

			Import_Data_Counters=import_data(Users[i,1],Starting_Follower_Id_Of_Next_Page,Twitter_Token,Followers_Requests_Available,idcnt,Followee)
			Starting_Follower_Id_Of_Next_Page=Import_Data_Counters[[1]]
			Followers_Requests_Available=Import_Data_Counters[[2]]
			idcnt=Import_Data_Counters[[3]]

			if (idcnt%%10000==0) print(idcnt)
        }
    }
}
Get_Followers('TomSteyer')
Get_Followers('PeteButtigieg')
Get_Followers('BernieSanders')
Get_Followers('SenSanders')
Get_Followers('SenAmyKlobuchar')
Get_Followers('TulsiPress')
Get_Followers('TomSteyer')
Get_Followers('TulsiGabbard')
Get_Followers('AmyKlobuchar')
Get_Followers('AndrewYang')
Get_Followers('MikeBloomberg')
Get_Followers('ewarren')
Get_Followers('JoeBiden')
Get_Followers('SenWarren')
