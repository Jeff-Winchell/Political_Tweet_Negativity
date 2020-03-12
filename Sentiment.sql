Use Test
Go
Set NoCount On
Declare @Return_status int
Set @Return_Status=-1
While @Return_Status<>0 Begin
	Begin Transaction
	Exec @Return_Status = Tweet_Sentiment 0
	Commit
End