With One_Cand As
(Select Follower,Min(Candidate) As Candidate
			From dbo.Candidate_Follower
			Group By Follower
			Having Count(*)=1)
Select Candidate,
		Known_Active_Tweets+(Known_Active_Tweets/Known_Active_Status_Tweets)*Unknown_Active_Status_Tweets As Estimated_Tweets,
		Known_Active_Tweeters+(Known_Active_Tweeters/Known_Active_Status_Tweeters)*Unknown_Active_Status_Tweeters As Estimated_Tweeters,
		Known_Active_Tweets As Scored_Tweets,
		Known_Active_Tweeters As Scored_Tweeters
	From
		(Select One_Cand.Candidate,
				Sum(Case When Inactive=0 And [user].Tweets>0 Then 1. Else 0. End) As Known_Active_Tweeters,
				Sum(Case When Inactive Is Not Null And [User].Tweets>0 Then 1. Else 0. End) As Known_Active_Status_Tweeters,
				Sum(Case When Inactive Is Null And [user].Tweets>0 Then 1. Else 0. End ) As Unknown_Active_Status_Tweeters,
				Sum(Tweeter_Score.Tweets) As Known_Active_Tweets,
				Sum(Case When Inactive Is Not Null And [user].Tweets>0 Then [User].Tweets Else 0. End ) As Known_Active_Status_Tweets,
				Sum(Case When Inactive Is Null And [user].Tweets>0 Then [User].Tweets Else 0. End ) As Unknown_Active_Status_Tweets
			From [user] 
					Inner Join
				One_Cand
						On One_Cand.Follower=[User].Id
					Left Outer Join
				dbo.Tweeter_Score
						On [User].Id=Tweeter_Score.Tweeter_Id
			Where One_Cand.Candidate is Not Null
			Group By One_Cand.Candidate) Temp




With One_Cand As
(Select Follower,Min(Candidate) As Candidate
			From dbo.Candidate_Follower
			Group By Follower
			Having Count(*)=1)
Select One_Cand.Candidate,
				Sum(Case When Inactive=0 And [user].Tweets>0 Then 1. Else 0. End) As Known_Active_Tweeters,
				Sum(Case When Inactive Is Not Null And [User].Tweets>0 Then 1. Else 0. End) As Known_Active_Status_Tweeters,
				Sum(Case When Inactive Is Null And [user].Tweets>0 Then 1. Else 0. End ) As Unknown_Active_Status_Tweeters,
				Sum(Tweeter_Score.Tweets) As Known_Active_Tweets,
				Sum(Case When Inactive Is Not Null And [user].Tweets>0 Then [User].Tweets Else 0. End ) As Known_Active_Status_Tweets,
				Sum(Case When Inactive Is Null And [user].Tweets>0 Then [User].Tweets Else 0. End ) As Unknown_Active_Status_Tweets
				Into #Foo
			From [user] 
					Inner Join
				One_Cand
						On One_Cand.Follower=[User].Id
					Left Outer Join
				dbo.Tweeter_Score
						On [User].Id=Tweeter_Score.Tweeter_Id
			Group By One_Cand.Candidate

Select Candidate,
		fORMAT(Known_Active_Tweets+(Known_Active_Tweets/Known_Active_Status_Tweets)*Unknown_Active_Status_Tweets,'N0') As Estimated_Tweets,
		fORMAT(Known_Active_Tweeters+(Known_Active_Tweeters/Known_Active_Status_Tweeters)*Unknown_Active_Status_Tweeters,'N0') As Estimated_Tweeters,
		fORMAT(Known_Active_Tweets,'N0') As Scored_Tweets,
		fORMAT(Known_Active_Tweeters,'N0') As Scored_Tweeters
 from #Foo
 where candidate Is not null
 With One_Cand as (
 Select Follower,Min(Candidate) As Candidate
			From dbo.Candidate_Follower
			Group By Follower
			Having Count(*)=1)
Update [User] Set One_Candidate=One_Cand.Candidate From [User] Inner Join One_Cand On [User].Id=One_Cand.Follower

Select candidate,count(*) FRom one_cand where candidate is not null group by candidate