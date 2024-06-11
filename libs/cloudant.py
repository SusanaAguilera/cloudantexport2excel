# -----------------------------------------------------------
# class to consult data CouchDB|Cloudant@IBM
#
# (C) 2024 Mayra Susana Aguilera Cardenas
# Released under MIT License (MIT)
# GH Repository https://github.com/SusanaAguilera/cloudantexport2excel 
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
        cloudantDBClass used to establish a connection with a cloudant instance && for data consulting && insertion..
        
        Attributes:
            my_database [dict]\n
            client [dict]\n
            rep [dict]\n
            passDB [String]\n
            userDB [String]\n
            urlDB [String]\n
        Methods:
            loadCredentials(self, DBVars={}): load .env file credentials in order to connect to the DB instance.\n
            connectToCloudant(self): used to get a Connection to Cloudant instance.\n
            connectToDB(self, dbName="", dataFrame={}): used to point a Cloudant database.\n
            queryDocument(self, queryname="", query={}, fields=[], sort="", index=""): used to perform a query to a Cloudant database.\n
            disconnectCloudant(self): disconnectCloudant used to lost the connection to Cloudant instance.\n
            insertDocument(self, doc={}): used insert a new doc into a Cloudant database.\n
            deleteDocument(self,id=""): used to delete an existing document in a Cloudant database.\n
            getDocument(self, id=""): getDocument used to get an existing single document in a Cloudant database.\n
            updateDocument(self, id, newVal): Update of an existing document in DB.\n
            searchInDocument(self, doc, newVal): Auxiliar function of updateDocument(), used to update values in nested Dictionaries.\n
            getView(self, designDoc, viewName): Retrieves a View.\n
            bulk(self, docs): Push a bunch of documents on one time over the database.\n
            chunkIt(self, seq, num): Chunks an Array very large into an Array of Arrays.\n
            bulkByBlocks(self, arrayToUpdate, blocks): If the initial Array of docs to bulk is pretty huge, this functions perfoms the bulk in phases.\n
            listDB(self): list the databases of a Cloudant/couch db Instance.\n
            destroyDB(self, dbName2Delete): Destroys a single Cloudant/couch db from the Instance.\n
            createDB(self, dbName2Create): Creates a single Cloudant/couch db from the Instance.\n
    """ 
    my_database = {}
    client = {}
    rep = {}
    passDB = ""
    userDB = ""
    urlDB = ""

    def loadCredentials(self, DBVars={}):
        """
            loadCredentials used to load .env file credentials in order to connect to the DB instance.\n
            Args:
                DBVars : Dict
            Returns:
                Boolean
        """
        try:
            self.userDB = DBVars["USERDB"]
            self.passDB = DBVars["PASSDB"]
            self.urlDB = DBVars["URLDB"]
            print('Credentials loaded ...')
            return True
        except Exception as e:
            print('Error at [libs][cloudantDBClass][loadCredentials].-')
            print(e);
            return (False);
        
    def connectToCloudant(self):
        """
            connectToCloudant used to get a Connection to Cloudant instance.\n
            Args:
            
            Returns:
                Boolean
        """
        try:
            self.client = Cloudant(self.userDB,
                                   self.passDB,
                                   url=self.urlDB,
                                   connect=True,
                                   auto_renew=True)
            self.client.session()
            print('Cloudant connected ...')
            return True;
        except Exception as e:
            print('Error at [libs][cloudantDBClass][connectToCloudant].-')
            print(e);
            return (False);
        
    def connectToDB(self, dbName=""):
        """
            connectToDB used to point a Cloudant database.\n
            Args:
                dbName : String
            Returns:
                Boolean
        """
        try:
            self.my_database = self.client[dbName]
            print("Connected to the data base .....",dbName)
            return True;
        except Exception as e:
            print('Error at [libs][cloudantDBClass][connectToDB].-')
            print(e);
            return (False);
            
    def queryDocument(self, queryname="", query={}, fields=[], sort="", index=""):
        """
            queryDocument used to perform a query to a Cloudant database.\n
            Args:
                queryname : String
                query : Dict
                fields : Array
                sort : String
                index : String
            Returns:
                Array
        """
        try:
            docs = self.my_database.get_query_result(query, fields=fields, use_index = index)
            queryResult = []
            for doc in docs:
                queryResult.append(doc)
            print('Succesfully retrieved ' + queryname + ' query')
            return queryResult
        except Exception as e:
            print('Error at [libs][cloudantDBClass][queryDocument].-')
            print(e);
            return [];

    def disconnectCloudant(self):
        """
            disconnectCloudant used to lost the connection to Cloudant instance.\n
            Args:
            
            Returns:
                Boolean
        """
        try:
            self.client.disconnect()
            print('Cloudant disconnected ...')
            return True
        except Exception as e:
            print('Error at [libs][cloudantDBClass][disconnectCloudant].-')
            print(e);
            return False

    def insertDocument(self, doc={}):
        """
            insertDocument used insert a new doc into a Cloudant database.\n
            Args:
                doc : Dict
            Returns:
                Boolean
        """
        try:
            my_document = self.my_database.create_document(doc)
            if my_document.exists():
                print('Document Inserted')
                return True
        except Exception as e:
            print('Error at [libs][cloudantDBClass][insertDocument].-')
            print(e);
            return False;

    def deleteDocument(self,id=""):
        """
            deleteDocument used to delete an existing document in a Cloudant database.\n
            Args:
                id : String
            Returns:
                Boolean
        """
        try:
            my_document = self.my_database[id]
            my_document.delete()
            if my_document.exists():
                print('Document erased ...')
                return True
        except Exception as e:
            print('Error at [libs][cloudantDBClass][deleteDocument].-')
            print(e);
            return False;
            
    def getDocument(self, id=""):
        """
            getDocument used to get an existing single document in a Cloudant database.\n
            Args:
                id : String
            Returns:
                Dict
        """
        try:
            my_document = self.my_database[id]
            if my_document:
                return my_document;
            else:
                return {};
        except Exception as e:
            print('Error at [libs][cloudantDBClass][getDocument].-')
            print(e);
            return {};

    def updateDocument(self, id="", newVal={}):
        """
            Update of an existing document in DB.
            Data must follow the JSON format which will indicates
            the path of the change E.g.: {"key":{"key":"value"}}.\n
            Args:
                id : String
                newVal : Dict
            Returns:
                Boolean
        """
        try:
            my_document = self.my_database[id]
            if my_document.exists():
                updated_doc = self.searchInDocument(my_document,newVal)
                updated_doc.save()
                print(id,' has been updated.')
                return True
        except Exception as e:
            print('Error at [libs][cloudantDBClass][updateDocument].-')
            print(e);
            return False;
    
    def searchInDocument(self, doc={}, newVal={}):
        """
            Auxiliar function of updateDocument(), used to update values in nested Dictionaries.\n
            Args:
                doc : Dict
                newVal : Dict
            Returns:
                Dict
        """
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
        except Exception as e:
            print('Error at [libs][cloudantDBClass][searchInDocument].-')
            print(e);
            return {};
    
    def getView(self, designDoc="", viewName=""):
        """
            Retrieves a View.\n
            Args:
                designDoc : String
                viewName : String
            Returns:
                Dict
        """
        try:         
            view = self.my_database.get_view_result('_design/'+designDoc, viewName, include_docs=True, reduce=False);
            return view;
        except Exception as e:
            print('Error at [libs][cloudantDBClass][getView].-')
            print(e);
            return({});
    
    def bulk(self, docs=[]):
        """
            Push a bunch of documents on one time over the database.\n
            Args:
                docs : []
            Returns:
                Boolean
        """
        try:
            self.my_database.bulk_docs(docs)
            print("Successfully bulk of "+str(len(docs))+" docs.")
            return True;
        except Exception as e:
            print('Error at [libs][cloudantDBClass][bulk].-')
            print(e);
            return False;

    def chunkIt(self, seq=[], num=0):
        """
            Chunks an Array very large into an Array of Arrays.\n
            Args:
                seq : Array
                num : Integer
            Returns:
                Array
        """
        try:
            avg = len(seq) / float(num)
            out = []
            last = 0.0
            while last < len(seq):    
                out.append(seq[int(last):int(last + avg)])
                last += avg
            return out
        except Exception as e:
            print('Error at [libs][cloudantDBClass][chunkIt].-')
            print(e);
            return [];
    
    def bulkByBlocks(self, arrayToUpdate=[], blocks=0):
        """
            If the initial Array of docs to bulk is pretty huge, this functions perfoms the bulk in phases.\n
            Args:
                arrayToUpdate : Array
                blocks : Integer
            Returns:
                None
        """
        try:
            arrayToBulk = self.chunkIt(arrayToUpdate, blocks)
            for arrayBulk in arrayToBulk:
                print("Docs to bulk "+str(len(arrayBulk)))
                if(self.bulk(arrayBulk)): 
                    print("bulk successfully")
        except Exception as e:
            print('Error at [libs][cloudantDBClass][bulkByBlocks].-')
            print(e);    
    
    def listDB(self):
        """
            list the databases of a Cloudant/couch db Instance.\n
            Args:
            
            Returns:
                Array
        """
        try:
            allDbs = self.client.all_dbs()
            return allDbs;
        except Exception as e:
            print('Error at [libs][cloudantDBClass][listDB].-')
            print(e); 
            return [];
    
    def destroyDB(self, dbName2Delete):
        """
            Destroys a single Cloudant/couch db from the Instance.\n
            Args:
                dbName2Delete : String
            Returns:
                None
        """
        try:
            self.client.delete_database(dbName2Delete);
            print("The database: "+dbName2Delete+" was destroyed successfully")
        except Exception as e:
            print('Error at [libs][cloudantDBClass][destroyDB].-')
            print(e);
            
    def createDB(self, dbName2Create):
        """
            Creates a single Cloudant/couch db from the Instance.\n
            Args:
                dbName2Create : String
            Returns:
                None
        """
        try:
            self.client.create_database(dbName2Create);
            print("The database: "+dbName2Create+" was created successfully")
        except Exception as e:
            print('Error at [libs][cloudantDBClass][createDB].-')
            print(e);

