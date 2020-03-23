Update [User] Set Male=0 Where (Contains(Name,'Near((she,hers),1)') Or Contains(Name,'Near((her,hers),1)') Or Contains(Name,'Near((she,her),1)')) And Male Is Null 
Update [User] Set Male=0 Where (Contains(Bio,'Near((she,hers),1)') Or Contains(Bio,'Near((her,hers),1)') Or Contains(Bio,'Near((she,her),1)')) And Male Is Null 
Update [User] Set Male=1 Where (Contains(Name,'Near((he,him),1)') Or Contains(Name,'Near((he,his),1)') Or Contains(Name,'Near((his,him),1)')) And Male Is Null 
Update [User] Set Male=1 Where (Contains(Bio,'Near((he,him),1)') Or Contains(Bio,'Near((he,his),1)') Or Contains(Bio,'Near((his,him),1)')) And Male Is Null 
Update [User] Set Male=0.5 Where (Contains(Name,'Near((they,them),1)') Or Contains(Bio,'Near((they,them),1)')) And Male Is Null 

Update [User] Set Supports='Bernie' Where Supports Is Null And Contains((Name,Bio),'Bernie2020 Or "BernieSanders*" Or FeelTheBern Or NotMeUs Or BernieOrBust Or SenSanders Or OurRevolution Or "Bernie Sanders" Or Near((Feel,Bern),2) Or Near(("Not Me",Us),1) Or "Bernie 2020"') 
Update [User] Set Supports='Warren' Where Supports Is Null And Contains((Name,Bio),'Warren2020 OR Liz2020 OR "ElizabethWarren*" Or TeamWarren Or SenWarren Or EWarren Or "Elizabeth Warren" Or "Senator Warren" Or "Warren 2020" Or "Team Warren"')
Update [User] Set Supports='Biden' Where Supports Is Null And Contains((Name,Bio),'"Biden*" Or "JoeBiden*" Or "Biden 2020" Or "Joe Biden"')
Update [User] Set Supports='Bloomberg' Where Supports Is Null And Contains((Name,Bio),'"MikeBloomberg*" Or Bloomberg2020 Or Mike2020 Or "Mike Bloomberg" or "Bloomberg 2020"')
Update [User] Set Supports='Tulsi' Where Supports Is Null And Contains((Name,Bio),'"Tulsi*" Or "TulsiGabbard*" Or TulsiPress Or "Tulsi Gabbard" Or "Tulsi 2020"')
Update [User] Set Supports='Buttigieg' Where Supports Is Null And Contains((Name,Bio),'Pete2020 Or "Buttigieg*" Or "PeteButtigieg%" Or TeamPete Or PeteForAmerica Or "Pete Buttigieg" Or Near((Pete,America),1) Or "Pete 2020" Or "Buttigieg 2020"')
Update [User] Set Supports='Klobuchar' Where Supports Is Null And Contains((Name,Bio),'Amy2020 Or "Klobuchar*" Or "AmyKlobuchar%" Or SenKlobuchar Or "Amy 2020" Or "Klobuchar 2020" Or "Amy Klobuchar" Or "Senator Klobuchar"')
Update [User] Set Supports='Steyer' Where Supports Is Null And Contains((Name,Bio),'"TomSteyer*" Or Steyer2020 Or NextGenAmerica Or TeamTom Or Need2Impeach Or "Tom Steyer" Or "Steyer 2020" Or "Team Tom"')
Update [User] Set Supports='Yang' Where Supports Is Null And Contains((Name,Bio),'Yang2020 Or "YangGang*" Or "AndrewYang*" Or HumanityFirst Or FreedomDividend Or "Yang 2020" Or "Andrew Yang" Or "Humanity First" Or "Yang Gang"')
Update [User] Set Supports='Trump' Where Supports Is Null And Contains((Name,Bio),'Trump2020 Or "DonaldTrump*" Or realDonaldTrump Or MAGA Or "Trump 2020"')

Truncate Table First_Name
	Insert Into First_Name
	Exec sp_execute_external_script @language=N'Python',
		@script=N'
import zipfile
myzip=zipfile.ZipFile(r"c:\politics\twitter_politician_supporters\names.zip")
Year,Name,Gender,Population=[],[],[],[]
for file_name in myzip.namelist():
    if file_name[-4:]==".txt":
        for row in myzip.open(file_name).readlines():
            cols=row.decode().split(",")
            Year.append(int(file_name[3:7]))
            Name.append(cols[0])
            Gender.append(cols[1])
            Population.append(cols[2].rstrip("\r\n"))
OutputDataSet=pandas.DataFrame(list(zip(Year, Name, Gender, Population)),columns=["Year","Name","Gender","Population"])'
Truncate Table Population_Adjustment
Insert Into Population_Adjustment
	Exec sp_execute_external_script @language=N'Python',
		@script=N'
OutputDataSet=pandas.read_csv(r"c:\politics\twitter_politician_supporters\Pop_Alive_in_2018_By_Birth_Year.csv")'
Truncate Table Name_Gender
Go
With Name_Gender_Population ([Name],Gender,Still_Alive_Population)
As 
(Select [Name],Gender,
		Avg(First_Name.[Population]*Population_Adjustment.Still_Alive_Fraction) As Still_Alive_Population
	From First_Name 
			Inner Join
		Population_Adjustment
				On First_Name.[Year]=Population_Adjustment.[Year]
	Group By [Name],Gender
)
Insert Into Name_Gender 
Select [Name],Cast(Round(Male_Population/(Male_Population+Female_Population),3) As Numeric(4,3)) As Male_Probability
	From
		(Select [Name],
			Sum(Case When Gender='M' Then Still_Alive_Population Else 0 End) As Male_Population,
			Sum(Case When Gender='F' Then Still_Alive_Population Else 0 End) As Female_Population
		From Name_Gender_Population
		Group By [Name]
		) Temp
	Where Male_Population+Female_Population>= 5
		--And Not Male_Population/(Male_Population+Female_Population) Between .01 And .99
	Order By 1
