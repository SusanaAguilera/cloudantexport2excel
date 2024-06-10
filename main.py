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
        with open('queries/getAssessments.json') as json_file:
            queries = json.load(json_file)
        dbCred = json.loads(os.getenv("DBSOURCE"))

        if (cloudant.loadCredentials(dbCred) and cloudant.connectToCloudant()):
            cloudant.connectToDB(dbCred["NAMEDB"])
            print("Successfully connected!")
            docs=[]    
            for query in queries:
                docs += cloudant.queryDocument("get data", queries[query]["selector"])
            print(f"{str(len(docs))} docs retrieved");
            docs_df = pd.DataFrame(docs);
            docs_df = docs_df.drop(columns=["_id","_rev","parentid"]);
            file_path = 'assessments'
            if(excelFormat.createWriter(file_path,"Assessments",docs_df)):
                print("Excel file created !!!!");
                print("Formatting the Excel file .....");
                excelFormat.setSheet("Assessments")
                excelFormat.setHeaderFormat({
                "bold": True,
                "text_wrap": True,
                "valign": "top",
                "fg_color": "#D7E4BC",
                "border": 1,
                },docs_df);
                excelFormat.closeWriter();
                print("Process End!!");
    except Exception as e: 
        print("Error at Main");
        print(e);

if __name__ == "__main__":
    main()