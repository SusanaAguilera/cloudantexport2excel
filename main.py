from libs.cloudant import cloudantDBClass
import json
import os

class cloudant2Excel:
    cloudant = cloudantDBClass()

    dbCred = json.loads(os.getenv("DBSOURCE"))

    if (cloudant.loadCredentials(dbCred) and cloudant.connectToCloudant()):
        cloudant.connectToDB(dbCred["NAMEDB"])
        print("Successfully connected!")

        query = {'IOT': 'IBM Americas'}
            
        
        data = cloudant.queryDocument("testing", query)
        print(len(data))
        

        