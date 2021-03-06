-- Check for enabling external scripts, openrowset, fulltext indexes,etc
Use Master
Go
Drop Database If Exists Politicians_Twitter_Followers
Go
Create Database Politicians_Twitter_Followers Collate LATIN1_GENERAL_100_CI_AS_SC_UTF8
Go
Use Politicians_Twitter_Followers
Go
Alter Database Politicians_Twitter_Followers Set Recovery Simple
Go
Create Table Population_Adjustment ([Year] Int Not Null Constraint PK_Population_Adjustment Primary Key,Still_Alive_Fraction Numeric(4,3) Not Null)
Go
Create Table First_Name ([Year] Int Not Null,[Name] VarChar(15) Not Null, 
	Gender Char(1) Not Null, [Population] Int Not Null,
	Constraint PK_First_Name Primary Key([Year],[Name],Gender))
Go
Create Table Name_Gender ([Name] VarChar(15) Not Null Constraint PK_Name_Gender Primary Key,Male_Probability Numeric(4,3) Not Null)
Go
Create Table [User] (
	Id BigInt Not Null Constraint PK_User Primary Key,
	Profile_Done As Case When Screen_Name Is Not Null Then 1 Else 0 End,
	Friends_Done Bit Null,
	Screen_Name VarChar(15) Null,
	[Name] VarChar(200) Collate LATIN1_GENERAL_100_CI_AS_SC_UTF8 Null,
	Bio VarChar(640) Collate LATIN1_GENERAL_100_CI_AS_SC_UTF8 Null,
	Profile_Image_URL VarChar(160) Null,
	Followers BigInt Null,
	[Following] BigInt Null,
	Tweets BigInt Null,
	Zero_Tweets As Case When Tweets=0 Then 1 Else 0 End,
	[Location] VarChar(640) Collate LATIN1_GENERAL_100_CI_AS_SC_UTF8 Null,
	Time_Zone VarChar(640) Collate LATIN1_GENERAL_100_CI_AS_SC_UTF8 Null,  --$time_zone$tzinfo_name --https://api.twitter.com/1.1/account/settings.json
	Geo_Enabled Bit Null,
	[Language] Char(2) Null, --language
	Trend_Location_Country VarChar(4) Null, -- $trend_location$countryCode
	Trend_Location_Name VarChar(640) Null, -- $trend_location$name
	Male Real Null,
	Supports VarChar(9) Null,
	Created_On DateTimeOffSet(0) Null,
	Inactive Bit Null,
	Politician Bit Constraint DF_Politician Default 0,
	Journalist Bit Constraint DF_Journaist Null,
	Inactive_Candidate Bit Null,
	Surrogate_Of BigInt Null Constraint FK_User_User Foreign Key References [User],
	Pundit Bit Null,
	Last_Tweeted_On Datetimeoffset(0) Null,
	Verified Bit Constraint DF_Verified Default 0,
	[Candidate] VarChar(9) Null,
	Added2TableOn DateTime Null Constraint DF_User_Added2TableOn Default GetDate()
	)
Go
Create Index Surrogate_Of On [User](Surrogate_Of)
Go
Create Table Tweet (
	Id BigInt Not Null Constraint PK_Tweet Primary Key,
	Tweeter_Id BigInt Null Constraint FK_Tweet_User Foreign Key References [User] On Delete Cascade,
	[Text] VarChar(1120) Collate LATIN1_GENERAL_100_CI_AS_SC_UTF8 Not Null,
	[Language] VarChar(3) Not Null,
	Is_Reply Bit Null,
	Computed Bit Null, -- Redundant (not calculated due to Indexed view requirements)=MSFT_Sentiment is Not Null and Textblob_sentiment is not null and language='en'
	In_Reply_To_User BigInt Null,
	In_Reply_To_Tweet BigInt Null,
	Retweet_Of_Tweet BigInt Null,
	Retweet_Of_User BigInt Null,
	Retweet_Count BigInt Null,
	Favorite_Count BigInt Null,
	TextBlob_Sentiment Real Null,
	Sentiment140 TinyInt Null,
	TreeBankSentiment Real Null,
	MSFT_Sentiment Real Null,
	Sentiment Real Null,
	Created_On DateTimeOffset(0) Not Null,
	Possibly_Truncated Bit Not Null Constraint DF_Possibly Default 0
) With (Data_Compression=Page)
Go
Create Index Tweeter_Id On Tweet(Tweeter_Id) Include(Id)
Go
Create Table Tweet_Hashtag (
	Tweet_Id BigInt Not Null Constraint FK_Hashtag_Tweet References Tweet On Delete Cascade,
	Hashtag VarChar(560) Not Null,
	Constraint PK_Tweet_Hashtag Primary Key(Hashtag,Tweet_Id)
	)
Go
Create Index Tweet_Hashtag_TweetId On Tweet_Hashtag(Tweet_Id)
Go
Create Table Nonreply_Tweet_User_Mention (
	Tweet_Id BigInt Not Null Constraint FK_Mention_Tweet References Tweet,
	Screen_Name VarChar(19) Not Null,
	Constraint PK_Nonreply_Tweet_User_Mention Primary Key(Tweet_Id,Screen_Name)
	)
Go
Create Table [Following] (
	Follower BigInt Not Null Constraint FK_Following_User_Follower Foreign Key References [User] On Delete Cascade,
	Followee BigInt Not Null Constraint FK_Following_User_Followee Foreign Key References [User],
	Added2TableOn DateTime Null Constraint DF_Following_Added2TableOn Default GetDate(),
	Candidate VarChar(9) Null, -- denormalized from [User] table
	Constraint Following_PK Primary Key(Follower,Followee)
	)
Go
Create Table [User2User] (
	Follower BigInt Not Null Constraint FK_User2User_User_Follower Foreign Key References [User] On Delete Cascade,
	Followee BigInt Not Null Constraint FK_User2User_User_Followee Foreign Key References [User],
	Added2TableOn DateTime Null Constraint DF_User2User_Added2TableOn Default GetDate()
	Constraint User2User_PK Primary Key(Follower,Followee)
	)
Go
Create Index FK_User2User_User_Followee On [User2User](Followee)
Go
Create Function dbo.Char30_To_DateTimeOffset(@Char30 Char(30)) RETURNS DateTimeOffset(0) With SchemaBinding AS Begin
	Return Cast(Convert(DateTime,SUBSTRING(@Char30,9,3)+SubString(@Char30,5,4)+Right(@Char30,4)+Substring(@Char30,11,9),109) As DateTimeOffset(0))
End
Go
Create Or Alter Function dbo.Significant_Digits_String(@Number Float, @Digits Int) Returns VarChar(max) With SchemaBinding As Begin
	Set @Number=Case When @Number = 0 Then 0 Else Round(@Number ,@Digits-1-Floor(Log10(Abs(@Number)))) End
	Return Case
		When @Number-Floor(@Number) = 0 Then LTrim(Str(@Number))
		Else Left(Cast(@Number As VarChar)+Replicate('0',@Digits),@Digits+Case When @Number>-1 And @Number<1 Then 2 Else 1 End+Case When @Number<0 Then 1 Else 0 End)
		End
End
Go

Create Or Alter Function dbo.Significant_Digits(@Number Float, @Digits Int) Returns Float With SchemaBinding As Begin
		Return Case When @Number = 0 Then 0 Else Round(@Number ,@Digits-1-Floor(Log10(Abs(@Number)))) End
End
Go
Create Or Alter Procedure Insert_Tweet (
	@Id VarChar(19),
	@Tweeter_Id VarChar(19),
	@Text VarChar(560),
	@Created_On Char(30),
	@Sentiment Real=Null,
	@Language VarChar(3),
	@Is_Reply Bit,
	@Retweet_Count Int,
	@Favorite_Count Int,
	@In_Reply_To_User VarChar(19)=Null,
	@In_Reply_To_Tweet VarChar(19)=Null,
	@Retweet_Of_Tweet VarChar(19)=Null,
	@Retweet_Of_User VarChar(19)=Null
	) As Begin
	If Not Exists(Select * From Tweet Where Id=Cast(@Id As BigInt))
		Insert Into Tweet(Id,Tweeter_Id,[Text],Created_On,Sentiment,[Language],Is_Reply,Retweet_Count,Favorite_Count,In_Reply_To_User,In_Reply_To_Tweet,Retweet_Of_Tweet,Retweet_Of_User)
			Values (Cast(@Id As BigInt),Cast(@Tweeter_Id As BigInt),@Text,dbo.Char30_To_DateTimeOffset(@Created_On),@Sentiment,@Language,@Is_Reply,@Retweet_Count,@Favorite_Count,Cast(@In_Reply_To_User As BigInt),Cast(@In_Reply_To_Tweet As BigInt),Cast(@Retweet_Of_Tweet As BigInt),Cast(@Retweet_Of_User As BigInt))
End
Go
Create Or Alter Procedure Insert_SearchedTweet (
    @SearchString VarChar(560),
	@Id VarChar(19),
	@Tweeter_Id VarChar(19),
	@Text VarChar(560),
	@Created_On Char(30),
	@Sentiment Real=Null,
	@Language VarChar(3),
	@Is_Reply Bit,
	@Retweet_Count Int,
	@Favorite_Count Int,
	@In_Reply_To_User VarChar(19)=Null,
	@In_Reply_To_Tweet VarChar(19)=Null,
	@Retweet_Of_Tweet VarChar(19)=Null,
	@Retweet_Of_User VarChar(19)=Null
	) As Begin
	If Not Exists(Select * From Tweet Where Id=Cast(@Id As BigInt)) Begin
		If Not Exists(Select * From [User] Where Id=Cast(@Tweeter_Id As BigInt)) Begin
			Insert Into Tweet(Id,Tweeter_Id,[Text],Created_On,Sentiment,[Language],Is_Reply,Retweet_Count,Favorite_Count,In_Reply_To_User,In_Reply_To_Tweet,Retweet_Of_Tweet,Retweet_Of_User)
				Values (Cast(@Id As BigInt),Null,@Text,dbo.Char30_To_DateTimeOffset(@Created_On),@Sentiment,@Language,@Is_Reply,@Retweet_Count,@Favorite_Count,Cast(@In_Reply_To_User As BigInt),Cast(@In_Reply_To_Tweet As BigInt),Cast(@Retweet_Of_Tweet As BigInt),Cast(@Retweet_Of_User As BigInt))
		End
		Insert Into Tweet(Id,Tweeter_Id,[Text],Created_On,Sentiment,[Language],Is_Reply,Retweet_Count,Favorite_Count,In_Reply_To_User,In_Reply_To_Tweet,Retweet_Of_Tweet,Retweet_Of_User)
			Values (Cast(@Id As BigInt),Cast(@Tweeter_Id As BigInt),@Text,dbo.Char30_To_DateTimeOffset(@Created_On),@Sentiment,@Language,@Is_Reply,@Retweet_Count,@Favorite_Count,Cast(@In_Reply_To_User As BigInt),Cast(@In_Reply_To_Tweet As BigInt),Cast(@Retweet_Of_Tweet As BigInt),Cast(@Retweet_Of_User As BigInt))
	End
	If Not Exists(Select * From Tweet_Hashtag Where Tweet_Id=Cast(@Id As BigInt) And Hashtag=@SearchString) Begin
		Insert Into Tweet_Hashtag (Tweet_Id,Hashtag)
			Values (Cast(@Id As BigInt),@SearchString)
	End
End
Go

Create Or Alter Procedure Insert_Tweet_Mention (
	@Id VarChar(19),
	@Screen_Name VarChar(19)) As Begin
	If Not Exists(Select * From Nonreply_Tweet_User_Mention Where Tweet_Id=Cast(@Id As BigInt))
		Insert Into Nonreply_Tweet_User_Mention (Tweet_Id,Screen_Name)
			Values (Cast(@Id As BigInt),@Screen_Name)
End
Go
Create Or Alter Procedure Insert_Politician (
	@Id VarChar(19),
	@Screen_Name VarChar(15),
	@Name VarChar(50)=Null,
	@Bio VarChar(160)=Null,
	@Followers Int,
	@Tweets Int,
	@Created_On Char(30),
	@Verified Bit
	) As Begin
	If Not Exists(Select * From [User] Where Id = Cast(@Id As BigInt))
		Insert Into dbo.[User] (Id,Screen_Name,[Name],Bio,Followers,Tweets,Created_On,Politician,Verified)
			Values (Cast(@Id As BigInt),@Screen_Name,@Name,@Bio,@Followers,@Tweets,dbo.Char30_To_DateTimeOffset(@Created_On),1,@Verified)
End
Go
Create or Alter Procedure Update_User (
	@Id VarChar(19),
	@Screen_Name VarChar(15),
	@Name VarChar(50)=Null,
	@Bio VarChar(640)=Null,
	@Followers Int,
	@Following Int,
	@Language Char(2),
	@Tweets Int,
	@Created_On Char(30),
	@Verified Bit,
	@Geo_Enabled Bit,
	@Location VarChar(640),
	@Time_Zone VarChar(640)
	) As Begin
	Update [User]
		Set Screen_Name=@Screen_Name,
			[Name]=@Name,
			Bio=@Bio,
			Created_On=dbo.Char30_To_DateTimeOffset(@Created_On),
			Followers=@Followers,
			[Following]=@Following,
			[Language]=@Language,
			Tweets=@Tweets,
			Verified=@Verified,
			Inactive=Case When @Tweets=0 Then 1 Else Null End,
			Geo_Enabled=@Geo_Enabled,
			[Location]=@Location,
			Time_Zone=@Time_Zone
		Where Id=Cast(@Id As BigInt)
End
Go
Create or Alter Procedure Delete_User (
	@Id VarChar(19)
	) As Begin
	Delete From [User2User] 
		Where Followee=Cast(@Id As BigInt)
	Delete From [User] Where Id=Cast(@Id As BigInt)
End
Go

Create Procedure Insert_Or_Update_User (
	@Id VarChar(19),
	@Screen_Name VarChar(15),
	@Name VarChar(50)=Null,
	@Bio VarChar(160)=Null,
	@Followers Int,
	@Following Int,
	@Language Char(2),
	@Tweets Int,
	@Created_On Char(30),
	@Verified Bit
	) As Begin
	If Exists(Select * From [User] Where Id=@Id)
		Update [User]
			Set Screen_Name=@Screen_Name,
				[Name]=@Name,
				Bio=@Bio,
				Created_On=dbo.Char30_To_DateTimeOffset(@Created_On),
				Followers=@Followers,
				[Following]=@Following,
				[Language]=@Language,
				Tweets=@Tweets,
				Verified=@Verified,
				Inactive=Case When @Tweets=0 Then 1 Else Null End
			Where Id=Cast(@Id As BigInt)
	Else
		Insert Into [User] (Id,Screen_Name,[Name],Bio,Created_On,Followers,[Following],[Language],Tweets,Verified,Inactive)
			Values (Cast(@Id As BigInt),@Screen_Name,@Name,@Bio,dbo.Char30_To_DateTimeOffset(@Created_On),@Followers,@Following,@Language,@Tweets,@Verified,Case When @Tweets=0 Then 1 Else Null End)
End
Go
Create Procedure Special_User(
	@Id VarChar(19),
	@Pundit Bit,
	@Journalist Bit,
	@Surrogate_Of VarChar(15)=Null) As Begin
	If @Surrogate_Of Is Null
		Update [User]
			Set Pundit=@Pundit,
				Journalist=@Journalist
			Where Id=Cast(@Id As BigInt)
	Else
		Update [User]
			Set Pundit=@Pundit,
				Journalist=@Journalist,
				@Surrogate_Of = (Select Id From [User] Where Screen_Name=@Surrogate_Of)
			Where Id=Cast(@Id As BigInt)
End
Go
Create Procedure User_Following (
	@Follower_Id VarChar(19),
	@Followee_Id VarChar(19)) As Begin
	If Not Exists(Select * From [Following] Where Follower=Cast(@Follower_Id As BigInt) And Followee=Cast(@Followee_Id As BigInt)) Begin
		If Not Exists(Select * From [User] Where Id = Cast(@Follower_Id As BigInt))
			Insert Into [User] (Id) Values (Cast(@Follower_Id As BigInt))
		Insert Into [Following] (Follower,Followee)
			Values (Cast(@Follower_Id As BigInt),Cast(@Followee_Id As BigInt))
	End
End
Go
Create Or Alter Procedure User_Friend (
	@Followee_Id VarChar(19),
	@Follower_Id VarChar(19)) As Begin
	If Exists(Select * From [User] Where Id=Cast(@Followee_Id As BigInt)) And Not Exists(Select * From [User2User] Where Follower=Cast(@Follower_Id As BigInt) And Followee=Cast(@Followee_Id As BigInt)) Begin
		Insert Into [User2User] (Follower,Followee)
			Values (Cast(@Follower_Id As BigInt),Cast(@Followee_Id As BigInt))
	End
End
Go
Create Procedure Inactive_User (
	@Id VarChar(19)
	) As Begin
	Update [User] Set Inactive=1 Where Id=Cast(@Id As BigInt)
End
Go
Create Procedure Last_Tweeted_On (
	@Id VarChar(19),
	@Created_On Char(30)
	) As Begin
	Update [User] Set Inactive=0,Last_Tweeted_On=dbo.Char30_To_DateTimeOffset(@Created_On) Where Id=Cast(@Id As BigInt)
End
Go

-- Change this path to where you have generated your files
Create or Alter Procedure Mass_Follower_Import(@Screen_Name VarChar(15),@Candidate VarChar(9)) As Begin
	Declare @Following As Table (Follower BigInt Not Null)
	Declare @PythonScript NVarChar(max)
	Set @PythonScript=N'
import os,pandas
files = []
for root, _, filelist in os.walk(''C:\\Politics\\Twitter_Politician_Supporters\\'+@Screen_Name+'\\''):
    for file in filelist:
        if ''.csv'' in file:
            files.append(os.path.join(root, file))
OutputList=[]
for file in files:
    #print(file)
    with open(file) as filehandle:
       OutputList.extend([int(i.replace(''"'','''')) for i in filehandle.read().splitlines()])
print(len(OutputList))
OutputDataSet=pandas.DataFrame(OutputList,columns=[''Follower''])
'

	Insert Into @Following
	Exec sp_execute_external_script @language=N'Python',
		@script=@PythonScript

	Select Distinct Follower Into #User From @Following A Where Not Exists(Select * From [User] Where [User].Id=A.Follower)
	Insert Into [User] (Id)
		Select * From #User

	Declare @Followee BigInt
	Select @Followee=Id From [User] Where Screen_Name=@Screen_Name

	Select Distinct Follower,@Followee As Followee Into #Following
		From @Following A
		Where Not Exists(Select *
							From [Following] B
							Where A.Follower=B.Follower And B.Followee=@Followee)
	Insert Into [Following] (Follower,Followee,Candidate)
		Select *,@Candidate From #Following
End
Go
Create or Alter Procedure Tweet_Sentiment(@Sample Bit=1) As Begin
	Set NoCount On
	Declare @Tweet_Sentiment As Table (Id VarChar(19) Not Null, MSFT_Sentiment Real Not Null, TextBlob_Sentiment Real Not Null)
	Declare @SQL NVarChar(max)
	If @Sample=1 Set @SQL=N'Select Top 40000 Cast(Id As VarChar(19)) As Id,Text From Tweet with (nolock) Tablesample (100000 rows) Where [Language]=''en'' And MSFT_Sentiment Is Null And TextBlob_Sentiment Is Null Order By NewID() Option(MAXDOP 4)'
	Else Set @SQL=N'Select Top 40000 Cast(Id As VarChar(19)) As Id,Text From Tweet with (nolock) Where [Language]=''en'' And MSFT_Sentiment Is Null And TextBlob_Sentiment Is Null Option(MAXDOP 4)'

	Insert Into @Tweet_Sentiment

	Exec sp_execute_external_script @language=N'Python',
		@script=N'import torch
print(torch.cuda.is_available())'
if (InputDataSet.shape[0]!=0):
	import microsoftml, textblob
	sentiment_scores = microsoftml.rx_featurize(
			data=InputDataSet,
			report_progress=0,
			verbose=0,
			ml_transforms=[microsoftml.get_sentiment(cols=dict(MSFT_Sentiment="Text"))])
	OutputDataSet=sentiment_scores[[''Id'',''MSFT_Sentiment'']].copy()

	TextBlob_Sentiment=[textblob.TextBlob(row[''Text'']).sentiment.polarity for index,row in InputDataSet.iterrows()]

	OutputDataSet[''TextBlob_Sentiment'']=TextBlob_Sentiment
else:
	OuputDataSet=pandas.DataFrame(columns=[''Id'',''MSFT_Sentiment'',''TextBlob_Sentiment''])
',
		@input_data_1=@SQL,
		@parallel=1,
		@params=N'@r_rowsPerRead int',
		@r_rowsPerRead=10000
	Update Tweet
		Set Tweet.MSFT_Sentiment = Sentiment.MSFT_Sentiment,
			Tweet.TextBlob_Sentiment=Sentiment.TextBlob_Sentiment,
			Computed=1
		From @Tweet_Sentiment Sentiment
			Inner Join
		Tweet
				On Cast(Sentiment.Id As BigInt) =Tweet.Id
	Return @@RowCount
End;
Go