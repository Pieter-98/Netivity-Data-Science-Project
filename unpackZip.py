import zipfile
import os
import json
import datetime
import sys
# To use this script, make sure the logs.zip file is in the same directory as this script. Then execute `python unpackZip.py` to run.

totalRequests = 0
amountOfErrors = 0
debug = False
timeStart = datetime.datetime.now().replace(microsecond=0)
sys.stdout.write("Running\n")
sys.stdout.flush()
# request is a Python dictionary which contains 3 keys, request, internal and context which can be accessed like 'request['request']'
# Be aware that the request key returns an array, so accessing the id is done like this: request['request'][0]['id']
with zipfile.ZipFile("logs.zip", "r") as f:
    for date in f.namelist():
        # if the path is a directory open it
        if(os.path.isfile(date)):
            # read contents of the file
            file = open(date, "r")
            # try expect to catch any exceptions
            try:
                # loop over each line
                for line in file:
                    totalRequests += 1
                    # parse the request
                    request = json.loads(file.readline())
            except:
                # log amount of errors
                amountOfErrors += 1
                # if debug is True, the program stops and reports the number of the request where it failed
                if(debug):
                    print("exception occurred at "+str(totalRequests)+"th request")
                    break;
            file.close()
timeEnd = datetime.datetime.now().replace(microsecond=0)
print("Script finished reading "+ str(totalRequests) + " requests.")
print(str(amountOfErrors)+" errors occurred.")
print("Took "+str(timeEnd - timeStart)+" seconds")