
import time

import mango


def mkdb():
    return mango.Database("http://127.0.0.1:5984/mango_test")


def setup():
    db = mkdb()
    db.recreate()
    time.sleep(0.25)
    docs = [{"_id": str(i), "int": i} for i in range(1, 11)]
    db.insert(docs)


def test_id_lookup():
    db = mkdb()
    doc = db.find_one({"_id": "1"})
    assert doc["_id"] == "1"
    assert "_rev" in doc
    assert doc["int"] == 1


def test_id_range():
    db = mkdb()
    docs = db.find({"_id": {"$and": [{"$gt": "1"}, {"$lte": "2"}]}})
    # returns "10" and "2"
    assert len(docs) == 2
    assert docs[0]["_id"] == "10"
    assert docs[1]["_id"] == "2"


def test_id_open_range():
    db = mkdb()
    docs = db.find({"_id": {"$gt": "1"}})
    assert len(docs) == 9

