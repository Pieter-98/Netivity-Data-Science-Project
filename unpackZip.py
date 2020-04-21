import zipfile
import os
import json
# To use this script, make sure the logs.zip file is in the same directory as this script. Then execute `python unpackZip.py` to run.


# request is a Python dictionary which contains 3 keys, request, internal and context which can be accessed like 'request['request']'
# Be aware that the request key returns an array, so accessing the id is done like this: request['request'][0]['id']
with zipfile.ZipFile("logs.zip", "r") as f:
    for date in f.namelist():
        if(os.path.isfile(date)):
            file = open(date)    
            for line in file:
                request = json.loads(file.readline())

