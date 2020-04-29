import unittest
from unittest.mock import Mock
from filterAndUploadInCurrentDir import *


class testFilteringMethods(unittest.TestCase):
    def setUp(self):
        self.rawRequest = """{"request": [
            {"id": "|myPAsLdsOuo=.f390a694_",
             "name": "GET navigation/Index", 
             "count": 25, 
             "responseCode": 200, 
             "success": true, 
             "url": "http://navigator-group1.tweakwise.com/navigation/768c5f16?tn_cid=333333-10000-190-213&tn_p=23&tn_sort=Populariteit&format=json", 
             "urlData": {"base": "/navigation/768c5f16", "host": "navigator-group1.tweakwise.com", "hashTag": "", "protocol": "http"}, 
             "durationMetric": {"value": 170475.0, "count": 25.0, "min": 6819.0, "max": 6819.0, "stdDev": 0.0, "sampledValue": 6819.0}}], 
                "internal": {"data": {"id": "28727460-2666-11e9-9d01-bd9c60a66059", "documentVersion": "1.61"}}, 
                "context": {"data": {"eventTime": "2019-02-01T21:12:39.6337594Z", "isSynthetic": false, "samplingRate": 4.0}, 
                            "cloud": {}, 
                            "device": {"type": "PC", "roleName": "Presentation.LegacyBus", "roleInstance": "TWN13", "screenResolution": {}}, 
                            "session": {"isFirst": false}, "operation": {"id": "myPAsLdsOuo=", "parentId": "myPAsLdsOuo=", "name": "GET navigation/Index"}, 
                            "location": {"clientip": "0.0.0.0", "continent": "Europe", "country": "United Kingdom"}, 
                            "custom": {"dimensions": [{"_MS.ProcessedByMetricExtractors": "(Name:'Requests', Ver:'1.0')"}, {"InstanceKey": "768c5f16"}]}}}"""
                            
        self.parsedRequest = json.loads(self.rawRequest)
        
        
    def tearDown(self):
        self.request = None
    
    def test_is_gateway_request(self):
        is_gateway = isGatewayRequest(self.parsedRequest)
        self.assertFalse(is_gateway)
    
    def test_is_legacy_request(self):
        is_legacy = isLegacyRequest(self.parsedRequest)
        self.assertTrue(is_legacy)
        
    def test_remove_incorrect_key(self):
        request = removeIncorrectKey(self.parsedRequest)
        self.assertEqual(request['context']['custom']['dimensions'],[{'InstanceKey': '768c5f16'}])      
        
class testFunctional(unittest.TestCase):
    def setUp(self):
        database['gateway'].drop()
        database['legacy'].drop()
    
    def tearDown(self):
        database['gateway'].drop()
        database['legacy'].drop()
            
    def testInsertTinySet(self):
        main()
        gatewayCount = database['gateway'].estimated_document_count()
        legacyCount = database['legacy'].estimated_document_count()
        self.assertEqual(9,gatewayCount)
        self.assertEqual(9,legacyCount)

        
if __name__ == "__main__":
    unittest.main()