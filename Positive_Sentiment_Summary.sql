Select Scored_Tweets.Candidate,
	Format(dbo.Significant_Digits(Followers*[% Active Tweeters]*[Tweets Per Scored Tweeter]*[% Very Positive Tweets],1),'N0') As [Total Very Positivity],
	Format(Followers,'N0') As Followers,
	dbo.Significant_Digits_String([% Active Tweeters],2)+'%' As [% Active Tweeters],
	dbo.Significant_Digits_String([Tweets Per Scored Tweeter],4) As [Tweets Per Scored Tweeter],
	dbo.Significant_Digits_String([% Very Positive Tweets],1)+'%' As [% Very Positive Tweets]
	From
		(Select Candidate,
				1.*Count(*)/Count(Distinct Tweeter_Id) As [Tweets Per Scored Tweeter],
				100.*Sum(Case When TextBlob_Sentiment > .75 Then 1. Else 0. End)/Count(*) As [% Very Positive Tweets]
			From
				(Select Distinct [User].[Candidate],Tweeter_Id,Tweet.Id,MSFT_Sentiment,TextBlob_Sentiment
					From Tweet with (nolock)
							Inner Join 
						[Following] with (nolock)
								On Tweeter_Id=Follower
							Inner Join
						[User] with (nolock)
								On [User].[Id]=Followee
					Where MSFT_Sentiment Is Not Null And TextBlob_Sentiment Is Not Null And Tweet.[Language]='en'
						And [User].Politician=1
				) Temp
			Group By Candidate
		) Scored_Tweets
			Inner Join
		(Select Candidate,
				Count(*) As Followers,
				Sum(Case When Inactive=0 Then 1.0 Else 0.0 End)/Sum(Case When Inactive Is Not Null Then 1.0 Else 0.0 End) As [% Active Tweeters]
			From
				(Select Distinct Pol.Candidate,[User].Id,[User].Inactive
					From [User] Pol with (nolock)
							Inner Join
						[Following] with (nolock)
								On Pol.Id=Followee
							Inner Join
						[User] with (nolock)
								On [Following].Follower=[User].Id
					Where Pol.Politician=1
				) Temp
			Group By Candidate
		) Followers
				On Followers.Candidate=Scored_Tweets.Candidate
	Order By Followers*[% Active Tweeters]*[Tweets Per Scored Tweeter]*[% Very Positive Tweets] Desc
Select Count(*) As [Scored Tweets],Count(Distinct Tweeter_Id) As [Scored Tweeters]
	From Tweet with (nolock)
	Where MSFT_Sentiment Is Not Null And TextBlob_Sentiment Is Not Null And Tweet.[Language]='en'
