Drop Table If Exists #FollowsCandidate
Select Distinct Candidate,Follower
		Into #FollowsCandidate
	From [Following]
			Inner Join
		[User]	
				On Followee=Id

Create Unique Clustered Index FollowsCandidate On #FollowsCandidate(Follower,Candidate)

Select A.Candidate,
		Format(Count(Distinct A.Follower),'N0') As Followers,
		Cast(Round(100.*Sum(Case When B.Candidate='Bernie' Then 1 Else 0 End)/Count(Distinct A.Follower),0) As TinyInt) As [% Bernie],
		Cast(Round(100.*Sum(Case When B.Candidate='Biden' Then 1 Else 0 End)/Count(Distinct A.Follower),0) As TinyInt)  As [% Biden],
		Cast(Round(100.*Sum(Case When B.Candidate='Bloomberg' Then 1 Else 0 End)/Count(Distinct A.Follower),0) As TinyInt)  As [% Bloomberg],
		Cast(Round(100.*Sum(Case When B.Candidate='Buttigieg' Then 1 Else 0 End)/Count(Distinct A.Follower),0) As TinyInt)  As [% Buttigieg],
		Cast(Round(100.*Sum(Case When B.Candidate='Klobuchar' Then 1 Else 0 End)/Count(Distinct A.Follower),0) As TinyInt)  As [% Klobuchar],
		Cast(Round(100.*Sum(Case When B.Candidate='Steyer' Then 1 Else 0 End)/Count(Distinct A.Follower),0) As TinyInt)  As [% Steyer],
		Cast(Round(100.*Sum(Case When B.Candidate='Trump' Then 1 Else 0 End)/Count(Distinct A.Follower),0) As TinyInt)  As [% Trump],
		Cast(Round(100.*Sum(Case When B.Candidate='Tulsi' Then 1 Else 0 End)/Count(Distinct A.Follower),0) As TinyInt)  As [% Tulsi],
		Cast(Round(100.*Sum(Case When B.Candidate='Warren' Then 1 Else 0 End)/Count(Distinct A.Follower),0) As TinyInt)  As [% Warren],
		Cast(Round(100.*Sum(Case When B.Candidate='Yang' Then 1 Else 0 End)/Count(Distinct A.Follower),0) As TinyInt)  As [% Yang]
	From #FollowsCandidate A
			Inner Join
		#FollowsCandidate B
				On A.Follower=B.Follower
	Group By A.Candidate
	Order By 1
Drop Table If Exists #FollowsCandidate