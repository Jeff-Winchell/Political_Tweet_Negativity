Use [Politicians_Twitter_Followers]
Go
Create Index Politician_Id On [User](Id) Include(Candidate,Screen_Name,Inactive_Candidate) Where Politician=1
Go
Create Index Done_Id On [User](Id) Where Profile_Done=0
Go
Create Index Id_Inactive On [User](Id) Include (Inactive,Zero_Tweets,Profile_Done)
Go
--Create Index In_Reply_To_User On Tweet(In_Reply_To_User) Include(Tweeter_Id,TextBlob_Sentiment,MSFT_Sentiment)
--Go
--Create Index Tweeter_Retweeter_Replyee On Tweet(Tweeter_Id) Include(Id,In_Reply_To_User,Retweet_of_User)
--Go
--Create Index Tweeter_Id_Plus On Tweet(Tweeter_Id) Include(MSFT_Sentiment,TextBlob_Sentiment,Id) 
--	Where MSFT_Sentiment Is Not Null And TextBlob_Sentiment Is Not Null And Tweet.[Language]='en'
Create View dbo.Tweet_Score With SchemaBinding As 
Select Tweeter_Id,Id,MSFT_Sentiment,TextBlob_Sentiment
	From dbo.Tweet
	Where Computed=1
Go
Create Unique Clustered Index AK_Tweet_Score On dbo.Tweet_Score(Tweeter_Id,Id)
Go
Create Index FK_Following_User_Followee On [Following](Followee) Include(Added2TableOn)
Go
--Create Index Tweeter_Sentiment On Tweet(Tweeter_Id)
--	Include(MSFT_Sentiment,TextBlob_Sentiment,In_Reply_To_User,Retweet_Of_User,Id)
--	Where TextBlob_Sentiment Is Not Null And MSFT_Sentiment Is Not Null And [Language]='en'
GO
Create Index Supports On [User](Supports)
Go
Create Index Screen_Name_Id On [User](Screen_Name,Id) Include ([Name])
Go
Create Index Politician On [User](Politician)
Go



