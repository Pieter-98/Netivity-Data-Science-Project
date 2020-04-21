import zipfile
import os
import json
import datetime
# To use this script, make sure the logs.zip file is in the same directory as this script. Then execute `python unpackZip.py` to run.

totalRequests = 0
amountOfErrors = 0
debug = False
timeStart = datetime.datetime.now().replace(microsecond=0)
# request is a Python dictionary which contains 3 keys, request, internal and context which can be accessed like 'request['request']'
# Be aware that the request key returns an array, so accessing the id is done like this: request['request'][0]['id']
with zipfile.ZipFile("logs.zip", "r") as f:
    for date in f.namelist():
        if(os.path.isfile(date)):
            file = open(date)
            try:
                for line in file:
                    totalRequests += 1
                    request = json.loads(file.readline())
            except:
                amountOfErrors += 1
                if(debug):
                    print("exception occurred at "+str(i)+"th request")
                    break;
timeEnd = datetime.datetime.now().replace(microsecond=0)
print("Script finished reading"+ str(totalRequests) + " requests.")
print(str(amountOfErrors)+" errors occurred.")
print("Took "+str(timeEnd - timeStart)+" seconds")