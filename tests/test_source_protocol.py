import unittest
from lab.http import serving
from lab.provider import Provider
from lab.demo import request
class SourceProtocol(unittest.TestCase):
    def test_source_roundtrip(self):
        with serving(Provider()) as base:
            result=request(base,'/graphql',{'operationName': 'OpenBatch'})
            self.assertIsNotNone(result)

    def test_inner_failure_keeps_outer_contract_and_retry(self):
        with serving(Provider()) as base:
            query={'operationName':'Measurements','variables':{'revision':'r1','experiment':'expA','after':'1'}}
            first=request(base,'/graphql',query)
            self.assertIsNone(first['data']['experiment']['measurements'])
            self.assertEqual(first['errors'][0]['path'],['experiment','measurements'])
            second=request(base,'/graphql',query)
            self.assertFalse(second['data']['experiment']['measurements']['pageInfo']['hasNextPage'])
            request(base,'/control',{'expire':True})
            self.assertEqual(request(base,'/graphql',query)['errors'][0]['extensions']['code'],'EXPIRED')
