import zipfile
import os
import json
import datetime
import sys
import shutil
import traceback
import pymongo
# To use this script, make sure the logs.zip file is in the same directory as this script. Then execute `python unpackZip.py` to run.

mongodbclient = pymongo.MongoClient("mongodb://localhost:27017/")
database = mongodbclient['netivity']

filename = "logs.zip"

totalRequests = 0
amountOfErrors = 0
debug = False
totalStoredRequests = 0

timeStart = datetime.datetime.now().replace(microsecond=0)

sys.stdout.write("Running..\n")
sys.stdout.flush()

# if an operation id is already is the list from gateway operation ids the legacy request should not be stored.
def saveIfOperationIdNotInList(line):
    global totalStoredRequests
    # parse the request
    request = json.loads(line)
    if(isLegacyRequest(request)):
        operationId = request['context']['operation']['id']
        if(database['context'].find({'operation.id':operationId}).count_documents() == 0):
            totalStoredRequests += 1
            storeDocuments(request)
            
def storeDocuments(request):
    # Remove _MS.ProcessedByMetricExtractors because the key is not correct. It is not required right now
    for element in request['context']['custom']['dimensions']:
        if "_MS.ProcessedByMetricExtractors" in element:
            request['context']['custom']['dimensions'].remove(element)
    database['context'].insert_one(request['context'])
    database['requests'].insert_one(request['request'][0])
    database['internal'].insert_one(request['internal'])        


def isGatewayRequest(request):
    return request['context']['device']['roleName'] == "Presentation.Gateway"


def isLegacyRequest(request):
    return request['context']['device']['roleName'] == "Presentation.LegacyBus"


def parseAndSaveLegacyRequest(file, filepath):
    try:
        # unpack the file
        content = file.extract(filepath)
        if not os.path.isfile(content):
            return
        # open the file
        logsZip = open(content)
        for line in logsZip:
            # parse the request
            saveIfOperationIdNotInList(line)
        logsZip.close()
        # remove the unpacked file
        os.remove(content)            
    except:
        if debug:
            print(traceback.format_exc())


def processGatewayRequest(line):
    global totalRequests
    global totalStoredRequests
    totalRequests += 1
    # parse the request
    request = json.loads(line)
    if(isGatewayRequest(request)):
        storeDocuments(request)
        totalStoredRequests += 1


def processRequests(file, filepath):
    global amountOfErrors
    try:
        # unpack the file
        content = file.extract(filepath)
        # if the path is not a file, return
        if not os.path.isfile(content):
            return
        # open the file
        logsZip = open(content)
        # read every line in the file
        for line in logsZip:
            processGatewayRequest(line)    
        # close the file to avoid possible memory leaks            
        logsZip.close()
        # remove the unpacked file
        os.remove(content)                    
    except:
        if debug:
            print(traceback.format_exc())
        amountOfErrors += 1


# request is a Python dictionary which contains 3 keys, request, internal and context which can be accessed like 'request['request']'
# Be aware that the request key returns an array, so accessing the id is done like this: request['request'][0]['id']
def main():
    with zipfile.ZipFile(filename, "r") as file:
        for blobfilepath in file.namelist():
            processRequests(file, blobfilepath)
    # Loop twice because this is more efficient than going through all requests every time we look up an operation id. This way is 2n instead of n^2
    # Here we check if there are Legacy requests that do not have a gateway identical request. If there is no identical gateway request, the legacy request will be stored.
        for blobfilepath in file.namelist():
            parseAndSaveLegacyRequest(file, blobfilepath)
    # Remove logs directory and all files in it because the function unpacks every file to that directory.
    shutil.rmtree("logs", ignore_errors=True)          
main()

timeEnd = datetime.datetime.now().replace(microsecond=0)
print("Script finished reading " + str(totalRequests) + " requests.")
print("Saved "+ str(totalStoredRequests)+" requests")
print(str(amountOfErrors)+" errors occurred.")
print("Took "+ str(timeEnd - timeStart)+" seconds")