Select Candidate,
	Format(Count(*),'N0') As Tweets,
	Format(Count(Distinct Tweeter_Id),'N0') As Tweeters,
	dbo.Significant_Digits_String(100*Avg(MSFT_Sentiment),2)+'%' As [Avg % Chance Pos (MSFT)],
	dbo.Significant_Digits_String(Avg(TextBlob_Sentiment),2) As [Avg Sentiment (Textblob)],
	dbo.Significant_Digits_String(100.*Sum(Case When MSFT_Sentiment Between 1./3 And .4 Then 1. Else 0. End)/Count(*),1)+'%' As [% Low Chance Pos (MSFT)],
	dbo.Significant_Digits_String(100.*Sum(Case When MSFT_Sentiment < 1./3 Then 1. Else 0. End)/Count(*),1)+'%' As [% Very Low Chance Pos (MSFT)],
	dbo.Significant_Digits_String(100.*Sum(Case When TextBlob_Sentiment Between -.75 And -.5 Then 1. Else 0. End)/Count(*),1)+'%' As [% Negative (TextBlob)],
	dbo.Significant_Digits_String(100.*Sum(Case When TextBlob_Sentiment < -.75 Then 1. Else 0. End)/Count(*),1)+'%' As [% Very Negative (TextBlob)]
	From
		(Select Distinct Supports As Candidate,Tweeter_Id,Tweet.Id,MSFT_Sentiment,TextBlob_Sentiment
			From Tweet with (nolock)
					Inner Join
				[User] with (nolock)
						On [User].Id=Tweet.Tweeter_Id
			Where MSFT_Sentiment Is Not Null And TextBlob_Sentiment Is Not Null And Tweet.[Language]='en'
				) Temp
	Group By Candidate
	Order By Sum(Case When MSFT_Sentiment < 1./3 Then 1. Else 0. End)/Count(*) Desc

Select Count(*) As [Scored Tweets],Count(Distinct Tweeter_Id) As [Scored Tweeters] 
	From Tweet with (nolock)
	Where MSFT_Sentiment Is Not Null And TextBlob_Sentiment Is Not Null And Tweet.[Language]='en'
		And Tweeter_Id In (Select Id From [User] Where	Supports Is Not Null)