import zipfile
import os
import json
import datetime
import sys
import shutil
import traceback
import pymongo
import cProfile
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
    
# Remove incorrect key _MS.ProcessedByMetricExtractors
def removeIncorrectKey(request):
    for element in request['context']['custom']['dimensions']:
        if "_MS.ProcessedByMetricExtractors" in element:
            request['context']['custom']['dimensions'].remove(element)
    return request

# Detect if request is gateway request
def isGatewayRequest(request):
    return request['context']['device']['roleName'] == "Presentation.Gateway"

# Detect if request is legacy request
def isLegacyRequest(request):
    return request['context']['device']['roleName'] == "Presentation.LegacyBus"

# Parse and save a legacy request
def parseAndSaveLegacyRequest(file, filepath):
    try:
        global totalStoredRequests
        # unpack the file
        content = file.extract(filepath)
        if not os.path.isfile(content):
            return
        # open the file
        logsZip = open(content)
        requests = []
        
        for line in logsZip:
            # Each line is a request, check if it already exists in the database, if it doesn't, store it.
            document = getLegacyRequestIfNotExists(line)
            if(document is not None):
                requests.append(document)
        if(len(requests) > 0):   
            totalStoredRequests += len(requests)
            database['legacy'].insert_many(requests)
        logsZip.close()
        # remove the unpacked file
        os.remove(content)            
    except:
        if debug:
            print(traceback.format_exc())


# if an operation id is already is the database the legacy request should not be stored.
def getLegacyRequestIfNotExists(line):
    # parse the request
    request = json.loads(line)
    if(isLegacyRequest(request)):
        operationId = request['context']['operation']['id']
        # Check if there already is a request with the same operation id
        if(database['gateway'].find({'context.operation.id' : operationId}).limit(1).count() == 0):
            # If there is no operation id, remove the incorrect key and return the request so it can be saved
            return removeIncorrectKey(request)

def processGatewayRequest(line):
    # parse the request
    request = json.loads(line)
    if(isGatewayRequest(request)):
        return removeIncorrectKey(request)


def processRequests(file, filepath):
    global amountOfErrors
    global totalStoredRequests
    global totalRequests    
    try:
        # unpack the file
        content = file.extract(filepath)
        # if the path is not a file, return
        if not os.path.isfile(content):
            return
        # open the file
        logsZip = open(content)
        # read every line in the file
        requests = []
        for line in logsZip:
            totalRequests += 1
            document = processGatewayRequest(line)
            if(document is not None): 
                requests.append(document)
        if(len(requests) > 0):   
            totalStoredRequests += len(requests)
            database['gateway'].insert_many(requests)
        # close the file to avoid possible memory leaks            
        logsZip.close()
        # remove the unpacked file
        os.remove(content)                    
    except:
        if debug:
            print(traceback.format_exc())
        amountOfErrors += 1

def main():
    # Set index on operation id, massively increases find speed.
    database['gateway'].create_index([("context.operation.id",pymongo.ASCENDING)])

    with zipfile.ZipFile(filename, "r") as file:
        # Insert all gateway requests. Afterwards loop through all legacy requests to check if the legacy request should be uploaded or not
        for blobfilepath in file.namelist():
            processRequests(file, blobfilepath)
            
        # Log storing gateway requests finished. Report time
        sys.stdout.write("Finished inserting gateway requests\n")
        timeEnd = datetime.datetime.now().replace(microsecond=0)
        sys.stdout.write("Took "+str(timeEnd - timeStart)+" seconds\n")        
        sys.stdout.write("Starting to insert legacy requests which are not in the database yet..\n")        
        sys.stdout.flush()            
    # Loop twice because this is more efficient than going through all requests every time we look up an operation id. This way is 2n instead of n^2
    # Here we check if there are Legacy requests that do not have a gateway identical request. If there is no identical gateway request, the legacy request will be stored.
        for blobfilepath in file.namelist():
            parseAndSaveLegacyRequest(file, blobfilepath)
    # Remove logs directory and all files in it because the function unpacks every file to that directory.
    shutil.rmtree("logs", ignore_errors=True)       
main()   
# cProfile.run('main()','stats') 

timeEnd = datetime.datetime.now().replace(microsecond=0)
print("Script finished reading " + str(totalRequests) + " requests.")
print("Saved "+ str(totalStoredRequests)+" requests")
print(str(amountOfErrors)+" errors occurred.")
print("Took "+ str(timeEnd - timeStart)+" seconds")