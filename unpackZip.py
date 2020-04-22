import zipfile
import shutil
import json
import datetime
import sys
import os
# To use this script, make sure the logs.zip file is in the same directory as this script. Then execute `python unpackZip.py` to run.

# If set to true, the program will report all errors
debug = False

amountOfErrors = 0
totalRequests = 0
timeStart = datetime.datetime.now().replace(microsecond=0)
sys.stdout.write("Running\n")
sys.stdout.flush()
    
def parseRequestsFromFile(file, filepath):
    # global parameters for statistics, might be changed in the future as global is not efficient
    global amountOfErrors, totalRequests
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
            # parse the line as json
            jsonfile = json.loads(line)
            totalRequests += 1
        # close the file to avoid possible memory leaks
        logsZip.close()
        # remove the unpacked file
        os.remove(content)
    except Exception as exc:
        if debug:
            print(exc)
        amountOfErrors += 1
        
# request is a Python dictionary which contains 3 keys, request, internal and context which can be accessed like 'request['request']'
# Be aware that the request key returns an array, so accessing the id is done like this: request['request'][0]['id']
def main():
    with zipfile.ZipFile("logs.zip") as f:
        for blobfilepath in f.namelist():
            # Parse all requests in the file
            parseRequestsFromFile(f, blobfilepath)
    # Remove logs directory and all files in it because the function unpacks every file to that directory.
    shutil.rmtree("logs", ignore_errors=True)        
main()
    
# Calculate time spent and report some statistics
timeEnd = datetime.datetime.now().replace(microsecond=0)
print("Script finished reading "+ str(totalRequests) + " requests.")
print(str(amountOfErrors)+" errors occurred.")
print("Took "+str(timeEnd - timeStart)+" seconds")
