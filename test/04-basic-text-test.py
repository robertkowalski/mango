
import mango
import user_docs


class BasicTextTests(mango.UserDocsTextTests):
    def test_simple(self):
        docs = self.db.find({"$text": "Stephanie"})
        assert len(docs) == 1
        assert docs[0]["name"]["first"] == "Stephanie"

    def test_with_integer(self):
        docs = self.db.find({"name.first": "Stephanie", "age": 48})
        assert len(docs) == 1
        assert docs[0]["name"]["first"] == "Stephanie"
        assert docs[0]["age"] == 48

    def test_with_boolean(self):
        docs = self.db.find({"name.first": "Stephanie", "manager": False})
        assert len(docs) == 1
        assert docs[0]["name"]["first"] == "Stephanie"
        assert docs[0]["manager"] == False

    def test_with_array(self):
        faves = ["Ruby", "C", "Python"]
        docs = self.db.find({"name.first": "Stephanie", "favorites": faves})
        assert docs[0]["name"]["first"] == "Stephanie"
        assert docs[0]["favorites"] == faves

    def test_lt(self):
        docs = self.db.find({"age": {"$lt": 22}})
        assert len(docs) == 0

        docs = self.db.find({"age": {"$lt": 23}})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"age": {"$lt": 33}})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"age": {"$lt": 34}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (1, 9)
 
    def test_lte(self):
        docs = self.db.find({"age": {"$lte": 21}})
        assert len(docs) == 0

        docs = self.db.find({"age": {"$lte": 22}})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"age": {"$lte": 33}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (1, 9)
    
    def test_eq(self):
        docs = self.db.find({"age": 21})
        assert len(docs) == 0
        
        docs = self.db.find({"age": 22})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"age": {"$eq": 22}})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"age": 33})
        assert len(docs) == 0

        docs = self.db.find({"age": 34})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 1

    def test_ne(self):
        docs = self.db.find({"age": {"$ne": 22}})
        assert len(docs) == len(user_docs.DOCS) - 1
        for d in docs:
            assert d["age"] != 22

        docs = self.db.find({"$not": {"age": 22}})
        assert len(docs) == len(user_docs.DOCS) - 1
        for d in docs:
            assert d["age"] != 22

    def test_gt(self):
        docs = self.db.find({"age": {"$gt": 77}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (3, 13)

        docs = self.db.find({"age": {"$gt": 78}})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 3

        docs = self.db.find({"age": {"$gt": 79}})
        assert len(docs) == 0

    def test_gte(self):
        docs = self.db.find({"age": {"$gte": 77}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (3, 13)

        docs = self.db.find({"age": {"$gte": 78}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (3, 13)

        docs = self.db.find({"age": {"$gte": 79}})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 3

        docs = self.db.find({"age": {"$gte": 80}})
        assert len(docs) == 0

    def test_and(self):
        docs = self.db.find({"age": "22", "manager": True})
        assert len(docs) == 1
        assert docs[0] == 9
        
        docs = self.db.find({"age": "22", "manager": False})
        assert len(docs) == 0

        docs = self.db.find({"$and": [{"age": 22}, {"manager": True}]})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"$and": [{"age": 22}, {"manager": False}]})
        assert len(docs) == 0

        docs = self.db.find({"$text": "Ramona", "age": 22})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"$and": [{"$text": "Ramona"}, {"age": 22}]})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"$and": [{"$text": "Ramona"}, {"$text": "Floyd"}]})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

    def test_or(self):
        docs = self.db.find({"$or": [{"age": 22}, {"age": 33}]})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (1, 9)

        q = {"$or": [{"$text": "Ramona"}, {"$text": "Stephanie"}]}
        docs = self.db.find(q)
        assert len(docs) == 2
        for d in docs:
            assert d["uesr_id"] in (1, 9)
        
        q = {"$or": [{"$text": "Ramona"}, {"age": 22}]}
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

    def test_and_or(self):
        q = {
            "age": 22,
            "$or": [
                {"manager": False},
                {"location.state": "Missouri"}
            ]
        }
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        q = {
            "$or": [
                {"age": 22},
                {"age": 43, "manager": True}
            ]
        }
        docs = self.db.find(q)
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (9, 10)

        q = {
            "$or": [
                {"$text": "Ramona"},
                {"age": 43, "manager": True}
            ]
        }
        docs = self.db.find(q)
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (9, 10)

    def test_nor(self):
        assert 1 == 0

    def test_in_with_value(self):
        docs = self.db.find({"age": {"$in": [1, 5]}})
        assert len(docs) == 0

        docs = self.db.find({"age": {"$in": [1, 5, 22]}})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"age": {"$in": [1, 5, 22, 31]}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (1, 9)

        docs = self.db.find({"age": {"$in": [22, 31]}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (1, 9)

        # Limits on boolean clauses?
        docs = self.db.find({"age": {"$in": range(1000)}})
        assert len(docs) == 15

    def test_in_with_array(self):
        vals = ["Random Garbage", 52, {"Versions": {"Alpha": "Beta"}}]
        docs = self.db.find({"favorites": {"$in": vals}})
        assert len(docs) == 1
        assert docs["user_id"] == 1
    
        vals = ["Lisp", "Python"]
        docs = self.db.find({"favorites": {"$in": vals}})
        assert len(docs) == 2 # There's more

        vals = [{"val1": 1, "val2": "val2"}]
        docs = self.db.find({"test_in": {"$in": vals}})
        assert len(docs) == 1
        assert docs["user_id"] == 2

    def test_nin_with_value(self):
        docs = self.db.find({"age": {"$nin": [1, 5]}})
        assert len(docs) == len(user_docs.DOCS)

        docs = self.db.find({"age": {"$nin": [1, 5, 22]}})
        assert len(docs) == len(user_docs.DOCS) - 1
        for d in docs:
            assert d["user_id"] != 9

        docs = self.db.find({"age": {"$nin": [1, 5, 22, 31]}})
        assert len(docs) == len(user_docs.DOCS) - 2
        for d in docs:
            assert d["user_id"] not in (1, 9)

        docs = self.db.find({"age": {"$nin": [22, 31]}})
        assert len(docs) == len(user_docs.DOCS) - 2
        for d in docs:
            assert d["user_id"] not in (1, 9)

        # Limits on boolean clauses?
        docs = self.db.find({"age": {"$nin": range(1000)}})
        assert len(docs) == 0

    def test_nin_with_array(self):
        vals = ["Random Garbage", 52, {"Versions": {"Alpha": "Beta"}}]
        docs = self.db.find({"favorites": {"$nin": vals}})
        assert len(docs) == len(user_docs.DOCS) - 1
        for d in docs:
            assert docs["user_id"] != 1

        vals = ["Lisp", "Python"]
        docs = self.db.find({"favorites": {"$nin": vals}})
        assert len(docs) == 0 # Fix

        vals = [{"val1": 1, "val2": "val2"}]
        docs = self.db.find({"test_in": {"$nin": vals}})
        assert len(docs) == len(user_docs.DOCS) - 1
        for d in docs:
            assert d["user_id"] != 2

    def test_all(self):
        vals = ["Ruby", "C", "Python", {"Versions": {"Alpha": "Beta"}}]
        docs = self.db.find({"favorites": {"$all": vals}})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 1

        # This matches where favorites either contains
        # the nested array, or is the nested array. This is
        # notably different than the non-nested array in that
        # it does not match a re-ordered version of the array.
        # The fact that user_id 14 isn't included demonstrates
        # this behavior.
        vals = [["Lisp", "Erlang", "Python"]]
        docs = self.db.find({"favorites": {"$all": vals}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (3, 9)

    def test_exists_field(self):
        docs = self.db.find({"exists_field": {"$exists": True}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (7, 8)
        
        docs = self.db.find({"exists_field": {"$exists": False}})
        assert len(docs) == len(user_docs.DOCS) - 2
        for d in docs:
            assert d["user_id"] not in (7, 8)
    
    def test_exists_array(self):
        docs = self.db.find({"exists_array": {"$exists": True}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (9, 10)

        docs = self.db.find({"exists_array": {"$exists": False}})
        assert len(docs) == len(user_docs.DOCS) - 2
        for d in docs:
            assert d["user_id"] not in (9, 10)

    def test_exists_object(self):
        docs = self.db.find({"exists_object": {"$exists": True}})
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (11, 12)
        
        docs = self.db.find({"exists_object": {"$exists": False}})
        assert len(docs) == len(user_docs.DOCS) - 2
        for d in docs:
            assert d["user_id"] not in (11, 12)
    
    def test_exists_object_member(self):
        docs = self.db.find({"exists_object.should": {"$exists": True}})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 11

        docs = self.db.find({"exists_object.should": {"$exists": False}})
        assert len(docs) == len(user_docs.DOCS) - 1
        for d in docs:
            assert d["user_id"] != 11

    def test_exists_and(self):
        q = {"$and": [
            {"manager": {"$exists": True}}
            {"exists_object.should": {"$exists": True}}
        ]}
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 11
    
        q = {"$and": [
            {"manager": {"$exists": False}}
            {"exists_object.should": {"$exists": True}}
        ]}
        docs = self.db.find(q)
        assert len(docs) == 0

        # Translates to manager exists or exists_object.should doesn't
        # exist, which will match all docs
        q = {"$not": q}
        docs = self.db.find(q)
        assert len(docs) == len(user_docs.DOCS)


    # test lucene syntax in $text


class ElemMatchTests(mango.FriendDocsTextTests):
    def test_elem_match(self):
        q = {
            "friends": {
                "$elemMatch": [
                    {"name.first": "Vargas"}
                ]
            }
        }
        docs = self.db.find(q)
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (0, 1)

        # A bit subtle here, but this first one is matching
        # name.first == Ochoa OR name.last == Burch, where the
        # next example switches it to an AND
        q = {
            "friends": {
                "$elemMatch": [
                    {"name.first": "Ochoa"},
                    {"name.last": "Burch"}
                ]
            }
        }
        docs = self.db.find(q)
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (1, 4)

        q = {
            "friends": {"$elemMatch": [
                {
                    "name.first": "Ochoa",
                    "name.last": "Burch"
                }
            ]}
        }
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 4

        # Check that we can do logic in elemMatch
        q = {
            "friends": {"$elemMatch": [
                {"name.first": "Ochoa", "type": "work"}
            ]}
        }
        docs = self.db.find(q)
        assert len(docs) == 1
        assert docs[0]["user_id"] == 4
        
        q = {
            "friends": {
                "$elemMatch": [{
                    "name.first": "Ochoa",
                    "$or": [
                        {"type": "work"},
                        {"type": "personal"}
                    ]
                }]
            }
        }
        docs = self.db.find(q)
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (1, 4)

        # Same as last, but using $in
        q = {
            "friends": {
                "$elemMatch": [{
                    "name.first": "Ochoa",
                    "type": {"$in": ["work", "personal"]}
                }]
            }
        }
        docs = self.db.find(q)
        assert len(docs) == 2
        for d in docs:
            assert d["user_id"] in (1, 4)
