# -----------------------------------------------------------
# class to consult data from Cloudant@IBM
#
# (C) 2024 MIRA dev Team
# Released under GNU Public License (GPL)
# GHE ORG https://github.ibm.com/CIO-Dev-Ops
# -----------------------------------------------------------

from cloudant.client import Cloudant
from cloudant.query import Query
from cloudant.replicator import Replicator
from cloudant.error import CloudantDatabaseException
from cloudant.error import CloudantException
from cloudant.error import CloudantReplicatorException
from dotenv import load_dotenv
import multitimer
import threading

load_dotenv()


class cloudantDBClass:
    """
        cloudantDBClass used to establish a connection with a cloudant instance && for data consulting && insertion.
    """
    my_database = {}
    client = {}
    rep = {}
    passDB = ""
    userDB = ""
    urlDB = ""
    timer = ""
    print_lock = threading.Lock()

    def loadCredentials(self, DBVars):
        """
            loadCredentials used to load .env file credentials in order to connect to the DB instance
            :param DBVars: Json with the DB credentials
            :return: boolean flag
        """
        try:
            self.userDB = DBVars["USERDB"]
            self.passDB = DBVars["PASSDB"]
            self.urlDB = DBVars["URLDB"]
            print('Credentials loaded ...')
            return True
        except:
            print("There was a problem loading the credentials...")
            return False

    def connectToCloudant(self):
        """
            connectToCloudant used to get a Connection to Cloudant instance
            :return: boolean flag
        """
        try:
            self.client = Cloudant(self.userDB,
                                   self.passDB,
                                   url=self.urlDB,
                                   connect=True,
                                   auto_renew=True)
            self.client.session()
            print('Cloudant connected ...')
            return True
        except:
            print("There was a problem connecting to Cloudant DB...")
            return False

    def connectToDB(self, dbName=""):
        """
            connectToDB used to point a Cloudant database
            :param dbName: String Name of the Database
            :return: boolean flag
        """
        try:
            self.my_database = self.client[dbName]
            print("Connected to the data base .....",dbName)
            return self.my_database
        except:
            print("There was a problem connecting to the data base...")
            return False
            
    def queryDocument(self, queryname="", query={}, fields=[], sort="", index=""):
        """
            queryDocument used to perform a query to a Cloudant database
            :param queryname: String Name of the query
            :param query: String Name of the query
            :param fields: Array of the fields desired to retrieve
            :param sort: Array of the Sort Value
            :param index: String Name of the Index
            :return: Array of Cloudant Documents(JSONs) 
        """
        try:
            docs = self.my_database.get_query_result(query, fields=fields, use_index = index)
            queryResult = []
            for doc in docs:
                queryResult.append(doc)
            print('Succesfully retrieved ' + queryname + ' query')
            return queryResult
        except OSError as err:
            print("OS error: {0}".format(err))
            return False

    def disconnectCloudant(self):
        """
            disconnectCloudant used to lost the connection to Cloudant instance
            :return: boolean flag
        """
        # Disconection from client
        try:
            self.client.disconnect()
            print('Cloudant disconnected ...')
            return True
        except:
            print("There was a problem connecting to the data base...")
            return False


    def insertDocument(self, doc={}):
        """
            insertDocument used insert a new doc into a Cloudant database
            :param doc: json document
            :return: boolean flag
        """
        try:
            my_document = self.my_database.create_document(doc)
            if my_document.exists():
                print('Document Inserted')
                return True
        except:
            print('There was an error inserting the document')
            return False

    def deleteDocument(self,id=""):
        """
            deleteDocument used to delete an existing document in a Cloudant database
            :param id: String document _id
            :return: boolean flag
        """
        try:
            my_document = self.my_database[id]
            my_document.delete()
            if my_document.exists():
                print('Document erased ...')
                return True
        except:
            print('There was an error deleting the document')
            
    def getDocument(self, id=""):
        """
            getDocument used to get an existing single document in a Cloudant database
            :param id: String document _id
            :return: json cloudant document
        """
        try:
            my_document = self.my_database[id]
            if my_document:
                return my_document;
            else:
                return False;
        except:
            print('There was an error getting the document')

    def updateDocument(self, id, newVal):
        # Update of an existing document in DB.
        # Data must follow the JSON format which will indicates
        # the path of the change E.g.: {"key":{"key":"value"}}
        try:
            my_document = self.my_database[id]
            if my_document.exists():
                updated_doc = self.searchInDocument(my_document,newVal)
                updated_doc.save()
                print(id,' has been updated.')
                return True
        except:
            print('There was an error updating the document')
    
    def searchInDocument(self, doc, newVal):
        # Update aux recursive function to search the path of the change
        try: 
            for key in newVal:
                searchKey = key
                searchVal = newVal[key]
            if type(searchVal) == dict:
                if searchKey in doc:
                    val = self.searchInDocument(doc[searchKey],searchVal)
                    doc[searchKey] = val
                    return doc
                else:
                    doc[searchKey] = searchVal
                    return doc
            elif type(searchVal) == str:
                doc[searchKey] = searchVal
                return doc
        except:
            print('Error while searching')
    
    def getView(self, designDoc, viewName):
        try:         
            view = self.my_database.get_view_result('_design/'+designDoc, viewName, include_docs=True, reduce=False)
            return view
        except:
            print("Error retrieving view")
    
    def bulk(self, docs):
        try:
            self.my_database.bulk_docs(docs)
            print("Successfully bulk of "+str(len(docs))+" docs.")
        except:
            print("Error bulk of  data")

    def chunkIt(self, seq, num):
        avg = len(seq) / float(num)
        out = []
        last = 0.0
        while last < len(seq):    
            out.append(seq[int(last):int(last + avg)])
            last += avg
        return out
    
    def bulkByBlocks(self, arrayToUpdate, blocks):
        arrayToBulk = self.chunkIt(arrayToUpdate, blocks)
        for arrayBulk in arrayToBulk:
            print("Docs to bulk "+str(len(arrayBulk)))
            if(self.bulk(arrayBulk)): 
                print("bulk successfully")
    
    def listDB(self):
        try:
            allDbs = self.client.all_dbs()
            return allDbs;
        except:
            print("Error Listing the db names")
    
    def destroyDB(self, dbName2Delete):
        try:
            self.client.delete_database(dbName2Delete);
            print("The database: "+dbName2Delete+" was destroyed successfully")
        except:
            print("Error destroying the database: "+dbName2Delete)
            
    def createDB(self, dbName2Create):
        try:
            self.client.create_database(dbName2Create);
            print("The database: "+dbName2Create+" was created successfully")
        except:
            print("Error creating the database: "+dbName2Create)
    
    def checkStatus(self, idReplication):
        try:
            state = self.rep.replication_state(idReplication)
            print("Replication : "+str(idReplication)+" state:"+str(state))
            if state != "running":
                self.timer.stop()
        except:
            print("Error checking the replication status")
            
    def replicate(self, dbNameSource, dbNameTarget,credSource, credTarget):
        try:
            print("\n************** REPLICATION PROCESS STARTED **************")
            print("\n*************** CLIENT CONNECTION ***************")
            sourceClient = Cloudant(credSource["USERDB"],
                                credSource["PASSDB"],
                                url=credSource["URLDB"],
                                connect=True,
                                auto_renew=True)
            sourceClient.session()
            print('Cloudant source client created')
            print("\n*************** Target CONNECTION ***************")
            targetClient = Cloudant(credTarget["USERDB"],
                                credTarget["PASSDB"],
                                url=credTarget["URLDB"],
                                connect=True,
                                auto_renew=True)
            targetClient.session()
            print('Cloudant target client created')
            self.rep = Replicator(sourceClient)
            replication = self.rep.create_replication(
                    source_db=sourceClient[dbNameSource],
                    target_db=targetClient[dbNameTarget],
                    continuous=False,
                    selector={"_deleted": {"$exists": False}}
                    )
            print('Replication document: {}'.format(replication["_id"]) )
            print('Wait until reaplication is completed.')
            self.timer = multitimer.MultiTimer(interval=10, function=self.checkStatus, kwargs={'idReplication':replication["_id"]})
            self.timer.start()
        except CloudantDatabaseException as error:
            print("There was a problem connecting to the data base...")
            print(error)
        except CloudantReplicatorException as error:
            print("There was a problem creating replication ...")
            print(error)
        except CloudantException as error:
            print("another problem on Cloudant...")
            print(error)
        except KeyError as err:
            print('{} Database not exist on the instance'.format(err))
    
    def getView4CleanUp(self, designDoc, viewName, qs):
        try:
            print("executing view : "+viewName+" of the designdoc: "+designDoc+" for the Q: "+qs[0])            
            viewResult = self.my_database.get_view_result('_design/'+designDoc, viewName, include_docs=True, key= qs[0])
            arrayView = []
            for doc in viewResult:
                doc['doc']['_deleted']= True;
                arrayView.append(doc['doc']);
            return arrayView
        except:
            print("Error retrieving view")
            
    def getView2Delete(self, designDoc, viewName):
        try:
            print("executing view : "+viewName+" of the designdoc: "+designDoc)            
            viewResult = self.my_database.get_view_result('_design/'+designDoc, viewName, include_docs=True)
            arrayView = []
            for doc in viewResult:
                doc['doc']['_deleted']= True;
                arrayView.append(doc['doc']);
            return arrayView
        except:
            print("Error retrieving view")
