Use Politicians_Twitter_Followers
Go
Create FullText Catalog Political_Twitter With Accent_Sensitivity = On As Default
Go
Create FullText StopList GenderSensitive From System Stoplist;
Go
Alter FullText StopList GenderSensitive Drop 'his' Language English;
Alter FullText StopList GenderSensitive Drop 'her' Language English;
Alter FullText StopList GenderSensitive Drop 'him' Language English;
Alter FullText StopList GenderSensitive Drop 'he' Language English;
Alter FullText StopList GenderSensitive Drop 'me' Language English;
Go
Create FullText Index
	On [User](
		Bio Language English Statistical_Semantics,
		[Name] Language English Statistical_Semantics)
	Key Index PK_User
	With (StopList Off)
	--With (StopList=GenderSensitive)
Go
Alter Fulltext Index On [User] Start Full Population;
Go
