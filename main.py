#Libs from local
from libs.cloudant import cloudantDBClass
from libs.formatExcel import excelFormatting
#libs from pip
import json
import os
import pandas as pd
from dotenv import load_dotenv


#load 
load_dotenv();

def main():
    try:
        cloudant = cloudantDBClass()
        excelFormat = excelFormatting()
        with open('queries/queries.json') as json_file:
            topics = json.load(json_file)
        dbCred = json.loads(os.getenv("DBSOURCE"))
        if (cloudant.loadCredentials(dbCred) and cloudant.connectToCloudant()):
            cloudant.connectToDB(dbCred["NAMEDB"]);
            print("Successfully connected!");
            for key in topics:
                name_equivalences = topics[key]["equivalneces"] if topics[key]["equivalneces"] else {};
                excelFileName = topics[key]["excelName"] if topics[key]["excelName"] else "";
                format4Excel = topics[key]["format4excel"] if (topics[key]["format4excel"]) else {} ;
                docs = cloudant.queryDocument("get data", topics[key]["selector"])
                docs_filtered = [];
                print(f"{str(len(docs))} docs retrieved");
                if str(name_equivalences) != '{}':
                    for doc in docs :
                        docs_filtered.append({name_equivalences[key]: value for key, value in doc.items() if key in name_equivalences})
                    docs_df = pd.DataFrame(docs_filtered);
                    if(excelFormat.createWriter(excelFileName,excelFileName,docs_df)):
                        print("Excel file created !!!!");
                        print("Formatting the Excel file .....");
                        excelFormat.setSheet(excelFileName)
                        excelFormat.setHeaderFormat(format4Excel["header"],docs_df);
                        excelFormat.closeWriter();
                        print("Process End!!");
    except Exception as e: 
        print("Error at Main");
        print(e);

if __name__ == "__main__":
    main()