
import mango
import user_docs


class OperatorTests(mango.DbPerClass):
    
    @classmethod
    def setUpClass(klass):
        super(OperatorTests, klass).setUpClass()
        user_docs.setup(klass.db)

    def test_all(self):
        docs = self.db.find({
                "manager": True,
                "favorites": {"$all": ["Lisp", "Python"]}
            })
        assert len(docs) == 3
        assert docs[0]["user_id"] == 2
        assert docs[1]["user_id"] == 12
        assert docs[2]["user_id"] == 9

    def test_all_non_array(self):
        docs = self.db.find({
                "manager": True,
                "location": {"$all": ["Ohai"]}
            })
        assert len(docs) == 0

    def test_elem_match(self):
        emdocs = [
            {
                "user_id": "a",
                "bang": [{
                    "foo": 1,
                    "bar": 2
                }]
            },
            {
                "user_id": "b",
                "bang": [{
                    "foo": 2,
                    "bam": True
                }]
            }
        ]
        self.db.save_docs(emdocs)
        docs = self.db.find({
            "_id": {"$gt": None},
            "bang": {"$elemMatch": {
                "foo": {"$gte": 1},
                "bam": True
            }}
        })
        assert len(docs) == 1
        assert docs[0]["user_id"] == "b"

    def test_in_operator_array():
        db = user_docs.mkdb()

        docs = db.find({
                "manager": True,
                "favorites": {"$in": ["Ruby", "Python"]}
            })
        assert len(docs) == 7
        assert docs[0]["user_id"] == 2
        assert docs[1]["user_id"] == 12

    def test_regex(self):
        docs = self.db.find({
                "age": {"$gt": 40},
                "location.state": {"$regex": "(?i)new.*"}
            })
        assert len(docs) == 2
        assert docs[0]["user_id"] == 2
        assert docs[1]["user_id"] == 10
