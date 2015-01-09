import mango
import limit_docs

class LimitTests(mango.LimitDocsTextTests):
    
    def test_limit_field(self):
        q = {"$or": [{"user_id" : {"$lt" : 10}}, {"filtered_array.[]": 1}]}
        docs = self.db.find(q, limit=10)
        assert len(docs) == 8
        for d in docs:
            assert d["user_id"] < 10
    
    def test_limit_field2(self):
        q = {"$or": [{"user_id" : {"$lt" : 20}}, {"filtered_array.[]": 1}]}
        docs = self.db.find(q, limit=10)
        assert len(docs) == 10
        for d in docs:
            assert d["user_id"] < 20

    def test_limit_field3(self):
        q = {"$or": [{"user_id" : {"$lt" : 100}}, {"filtered_array.[]": 1}]}
        docs = self.db.find(q, limit=1)
        assert len(docs) == 1
        for d in docs:
            assert d["user_id"] < 100

    def test_limit_field4(self):
        q = {"$or": [{"user_id" : {"$lt" : 0}}, {"filtered_array.[]": 1}]}
        docs = self.db.find(q, limit=35)
        assert len(docs) == 0
    
    # We reach our cap here of 50
    def test_limit_field5(self):
        q = {"$or": [{"user_id" : {"$lt" : 100}}, {"filtered_array.[]": 1}]}
        docs = self.db.find(q, limit=55)
        assert len(docs) == 50
        for d in docs:
            assert d["user_id"] < 100


        
        
        

 