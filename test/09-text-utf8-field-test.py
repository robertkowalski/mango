# -*- coding: utf-8 -*-
import mango
import user_docs

class Utf8Tests(mango.UserDocsTextTests):
    
    def test_utf8_field(self):
        q = {"«ταБЬℓσ»" : "utf-8"}
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 14

    def test_colons_arrays(self):
        q = {"utf8-1[]:string" : "string"}
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 13

        q = {"utf8-2[]:boolean[]" : True}
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 13

        q = {"utf8-3[]:number" : 9}
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 13

        q = {"utf8-3[]:null" : None}
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 13

    def test_http_url(self):
        q = {"utf8-1[]:string" : "string"}
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 13



        
        
        

 