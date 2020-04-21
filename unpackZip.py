import zipfile
import os
import json
import datetime
import sys
import base64
# To use this script, make sure the logs.zip file is in the same directory as this script. Then execute `python unpackZip.py` to run.

amountOfErrors = 0
totalRequests = 0
debug = True
timeStart = datetime.datetime.now().replace(microsecond=0)
sys.stdout.write("Running\n")
sys.stdout.flush()
    
def parseRequestsFromFile(filepath):
    global amountOfErrors, totalRequests
    try:
        data = f.read(blobfilepath).decode()
        requests = []
        request = ""
        for i in range(len(data)):
            if data[i] == '\n':
                totalRequests += 1
                jsonfile = json.loads(request)
                requests.append(jsonfile)
                request = ""
                continue
            request = request + data[i]
        return requests
    except:
        amountOfErrors += 1
        
# request is a Python dictionary which contains 3 keys, request, internal and context which can be accessed like 'request['request']'
# Be aware that the request key returns an array, so accessing the id is done like this: request['request'][0]['id']
with zipfile.ZipFile("logs.zip") as f:
    for blobfilepath in f.namelist():
        # if the extension of the file is .blob, read the file.
        if(os.path.splitext(blobfilepath)[1] == '.blob'):
            # read contents of the file
            requests = parseRequestsFromFile(blobfilepath)
    
# Calculate time spent and report some statistics
timeEnd = datetime.datetime.now().replace(microsecond=0)
print("Script finished reading "+ str(totalRequests) + " requests.")
print(str(amountOfErrors)+" errors occurred.")
print("Took "+str(timeEnd - timeStart)+" seconds")