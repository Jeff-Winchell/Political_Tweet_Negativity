import sqlmlutils
connection=sqlmlutils.ConnectionInfo(server="localhost",database="Test")
sqlmlutils.SQLPackageManager(connection).install("textblob")