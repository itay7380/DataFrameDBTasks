import asyncio

from base.utils.MemoryTracker import TrackMemory
from base.execution.CIndustrial import CIndustrial
from base.execution.CIndustrial import _s_INDUSTRIAL_FILE_PATH
from base.database.CMongoDb import CMongoDb
from base.execution.CWebServicesClient import CWebServicesClient
from base.web_server.CWebServer import CWebServer


@TrackMemory
def Main():
    # Task 1 + 2
    Industrial = CIndustrial()
    Industrial.CreateLookUp()
    Industrial.PopulateGroups()

    # Task 3
    # I deployed the MongoDB Compas Win server app
    print("Task 3:")
    MongoDb = CMongoDb(_s_INDUSTRIAL_FILE_PATH)
    MongoDb.InsertDocuments()
    MongoDb.FindDocuments()
    MongoDb.DropCollectionAndCloseConnection(False)
    print()

    # Task 4 + 5
    print("Tasks 4, 5:")
    WebServer = CWebServer("Industrial")
    WebServer.Start()
    WebServicesClient = CWebServicesClient()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(WebServicesClient.MakeApiRequests())


if __name__ == '__main__':
    Main()
