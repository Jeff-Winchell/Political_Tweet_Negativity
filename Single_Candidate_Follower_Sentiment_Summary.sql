Select Candidate,
	Format(Count(*),'N0') As Tweets,
	Format(Count(Distinct Tweeter_Id),'N0') As Tweeters,
	dbo.Significant_Digits_String(100*Avg(MSFT_Sentiment),2)+'%' As [Avg % Chance Pos (MSFT)],
	dbo.Significant_Digits_String(Avg(TextBlob_Sentiment),2) As [Avg Sentiment (Textblob)],
	dbo.Significant_Digits_String(100.*Sum(Case When MSFT_Sentiment Between 1./3 And .4 Then 1. Else 0. End)/Count(*),2)+'%' As [% Low Chance Pos (MSFT)],
	dbo.Significant_Digits_String(100.*Sum(Case When MSFT_Sentiment < 1./3 Then 1. Else 0. End)/Count(*),2)+'%' As [% Very Low Chance Pos (MSFT)],
	dbo.Significant_Digits_String(100.*Sum(Case When TextBlob_Sentiment Between -.75 And -.5 Then 1. Else 0. End)/Count(*),2)+'%' As [% Negative (TextBlob)],
	dbo.Significant_Digits_String(100.*Sum(Case When TextBlob_Sentiment < -.75 Then 1. Else 0. End)/Count(*),2)+'%' As [% Very Negative (TextBlob)]
	From dbo.Tweet_Score with (nolock)
			Inner Join 
		(Select Follower,Min(Candidate) As Candidate
			From dbo.Candidate_Follower with (nolock)
			Group By Follower
			Having Count(*)=1) One_Cand 
				On Tweeter_Id=Follower
	Group By Candidate
	Order By Sum(Case When MSFT_Sentiment < 1./3 Then 1. Else 0. End)/Count(*) Descs