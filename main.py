from libs.cloudant import cloudantDBClass

import json
import os

class cloudant2Excel:
    cloudant = cloudantDBClass()
    with open('queries/getAssessments.json') as json_file:
        queries = json.load(json_file)
    dbCred = json.loads(os.getenv("DBSOURCE"))

    if (cloudant.loadCredentials(dbCred) and cloudant.connectToCloudant()):
        cloudant.connectToDB(dbCred["NAMEDB"])
        print("Successfully connected!")
            
        for query in queries:
            data = cloudant.queryDocument("get data", queries[query]["selector"])
            print(len(data))
        

        