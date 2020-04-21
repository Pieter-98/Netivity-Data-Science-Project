import zipfile
import os
import json
# To use this script, make sure the logs.zip file is in the same directory as this script. Then execute `python unpackZip.py` to run.

# request is a Python dictionary which contains 3 keys, request, internal and context which can be accessed like 'request['request']'
# Be aware that the request key returns an array, so accessing the id is done like this: request['request'][0]['id']
 
with zipfile.ZipFile("logs.zip", "r") as f:
# loop over every path in the file    
    for date in f.namelist():
        # if the path is a directory open it
        if(os.path.isfile(date)):
            # read contents of the file
            file = open(date)
            # loop over each line
            for line in file:
                # parse the line as JSON, this is a single request
                request = json.loads(file.readline())

