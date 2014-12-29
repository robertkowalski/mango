import random
import time

import mango
import user_docs


def mkdb():
    return mango.Database("127.0.0.1", "5984", "mango_test")


def setup():
    user_docs.create_db_and_indexes()

# Basic text index search default field
def test_text_simple():
    db = mkdb()
    analyzer = "standard"
    _id = "text_simple"
    _name = "test_text_simple"
    ret = db.create_text_index(analyzer, name=_name, ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"$text":"Stephanie"})
    assert len(docs) == 2
    assert docs[1]["doc"]["name"]["first"] == "Stephanie"
    db.delete_index(idx["ddoc"], idx["name"], idx_type="text")

# Verifies that string, boolean, array, numbers are indexed when no fields
# are provided
def test_text_index_all():
    db = mkdb()
    analyzer = "standard"
    _id = "index_all"
    _name = "test_text_index_all"
    ret = db.create_text_index(analyzer, name=_name, ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"$and": [{"$text":"Stephanie"}, {"age":48}]})
    assert len(docs) == 2
    assert docs[1]["doc"]["age"] == 48
    docs = db.find({"$and": [{"$text":"Stephanie"}, {"manager":False}]})
    assert len(docs) == 2
    assert docs[1]["doc"]["manager"] == False
    docs = db.find({"$and": [{"$text":"Stephanie"}, {"favorites":["Ruby","C",
        "Python"]}]})
    assert len(docs) == 2
    assert docs[1]["doc"]["favorites"] == ["Ruby","C","Python"]
    db.delete_index(idx["ddoc"], idx["name"], idx_type="text")

# Verifies a user can selectively choose only a few fields to index
def test_text_index_fields_no_default():
    db = mkdb()
    analyzer = "standard"
    _default_field = {
        "enabled": False,
        "analyzer": "standard"
    }
    _fields = [{"field": "name.first", "type": "string", "analyzer" :"keyword"},
        {"field": "favorites.[]", "type": "string"},
        {"field": "manager", "type": "boolean"},
        {"field": "age", "type": "number"}
        ]
    _id = "index_fields_no_default"
    _name = "test_text_index_fields_no_default"
    ret = db.create_text_index(analyzer, default_field = _default_field,
        fields=_fields, name=_name, ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"$text":"Stephanie"})
    assert len(docs) == 1
    docs = db.find({"$or" : [{"$text": "Stephanie"},{"company":"Dreamia"}]})
    assert len(docs) == 1
    docs = db.find({"$or" : [{"$text": "Stephanie"},
        {"name.first":"Stephanie"}]})
    assert docs[1]["doc"]["name"]["first"] == "Stephanie"
    assert len(docs) == 2
    docs = db.find({"$or" : [{"$text": "Stephanie"},{"age":31}]})
    assert len(docs) == 2
    assert docs[1]["doc"]["age"] == 31
    docs = db.find({"$or" : [{"$text": "Stephanie"},{"manager":True}]})
    assert len(docs) == 12
    assert docs[1]["doc"]["manager"] == True
    docs = db.find({"$or" : [{"$text": "Stephanie"},{"favorites":["Ruby", "C",
        "Python"]}]})
    assert len(docs) == 2
    assert docs[1]["doc"]["favorites"] == ["Ruby", "C", "Python"]
    db.delete_index(idx["ddoc"], idx["name"], idx_type="text")

# # Verifies the default analyzer is keyword
def test_text_index_default_analyzer():
    db = mkdb()
    #analyzer = "email"
    _id = "index_default_analyzer"
    _name = "test_text_index_default_analyzer"
    ret = db.create_text_index(name=_name, ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"email":{"$text":"dreamia.com"}})
    assert len(docs) == 1
    docs = db.find({"email":{"$text":"stephaniekirkland@dreamia.com"}})
    assert len(docs) == 2
    assert docs[1]["doc"]["email"] == "stephaniekirkland@dreamia.com"
    db.delete_index(idx["ddoc"], idx["name"], idx_type="text")

# # Verifies user can modify default analyzer
def test_text_index_modify_default_analyzer():
    db = mkdb()
    analyzer = "simple"
    _id = "modify_default_analyzer"
    _name = "test_text_index_modify_default_analyzer"
    ret = db.create_text_index(analyzer,name=_name, ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"email":{"$text":"dreamia"}})
    assert len(docs) == 2
    assert docs[1]["doc"]["email"] == "stephaniekirkland@dreamia.com"
    db.delete_index(idx["ddoc"], idx["name"], idx_type="text")

# # Verify default_field analyzer and per_field analyzer
def test_text_index_perfield_default_field_analyzer():
    db = mkdb()
    analyzer = "keyword"
    _id = "perfield_default_field_analyzer"
    _name = "test_text_index_perfield_default_field_analyzer"
    _default_field = {
        "enabled": True,
        "analyzer": "keyword"
    }
    _fields = [{"field": "location.address.street", "type": "string",
    "analyzer" :"keyword"},
        {"field": "email", "type": "string", "analyzer" : "simple"},
        ]
    ret = db.create_text_index(analyzer,default_field=_default_field,
        fields=_fields,name=_name, ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"$text":"Evergreen"})
    assert len(docs) == 1
    docs = db.find({"email":{"$text":"dreamia"}})
    assert len(docs) == 2
    assert docs[1]["doc"]["email"] == "stephaniekirkland@dreamia.com"
    docs = db.find({"location.address.street":{"$text":"Huntington"}})
    assert len(docs) == 1
    docs = db.find({"location.address.street":{"$text":"Evergreen"}})
    assert len(docs) == 1
    docs = db.find({"location.address.street":{"$text":"\"Evergreen Avenue\""}})
    assert len(docs) == 2
    assert docs[1]["doc"]["location"]["address"]["street"] == "Evergreen Avenue"
    db.delete_index(idx["ddoc"], idx["name"], idx_type="text")


# Test $gte, $lte, $gt, $lt, $or, $and
def test_text_comparison_operators():
    db = mkdb()
    analyzer = "keyword"
    _id = "comparison_operators"
    _name = "test_text_comparison_operators"
    ret = db.create_text_index(analyzer, name=_name,
        ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"$or": [{"$text":"Stephanie"},{"$and":
        [{"age":{"$lte" : 31}}, {"age":{"$gte": 31}}]}]})
    assert len(docs) == 3
    assert docs[1]["doc"]["age"] == 31
    docs = db.find({"$or": [{"$text":"Floyd"},
        {"$and":[{"age":{"$lt" : 42}}, {"age":{"$gt": 0}}]}]})
    assert len(docs) == 4
    assert docs[1]["doc"]["age"] == 22
    assert docs[2]["doc"]["age"] == 33
    db.delete_index(_id, _name, idx_type="text")

# Test $in
def test_text_in_operator():
    db = mkdb()
    analyzer = "keyword"
    _id = "in_operator"
    _name = "test_text_in_operator"
    ret = db.create_text_index(analyzer, name=_name,
        ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"favorites" : {"$in":["Random Garbage", 52,
        {"Versions": {"Alpha": "Beta"}}]}})
    assert len(docs) == 2
    assert docs[1]["doc"]["favorites"] == ["Ruby","Python","C",
    {"Versions":{"Alpha":"Beta"}}]
    docs = db.find({"test_in" : {"$in":[{"val1": 1, "val2": "val2"}]}})
    db.delete_index(_id, _name, idx_type="text")

# Test $all
def test_text_all_operator():
    db = mkdb()
    analyzer = "keyword"
    _id = "all_operator"
    _name = "test_text_all_operator"
    ret = db.create_text_index(analyzer, name=_name,
        ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"favorites" : {"$all":["Ruby", "C", "Python",
        {"Versions": {"Alpha": "Beta"}}]}})
    assert len(docs) == 2
    assert docs[1]["doc"]["favorites"] == ["Ruby","Python","C",
    {"Versions":{"Alpha":"Beta"}}]
    docs = db.find({"favorites" : {"$all":[["Ruby", "C", "Python"]]}})
    assert len(docs) == 3
    assert docs[1]["doc"]["favorites"] == [["Ruby","C","Python"],"Erlang","C",
    "Erlang"]
    db.delete_index(_id, _name, idx_type="text")

# Test $exists
def test_text_exists_operator():
    db = mkdb()
    analyzer = "keyword"
    _id = "exists_operator"
    _name = "test_text_exists_operator"
    ret = db.create_text_index(analyzer, name=_name, ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"exists_field":{"$exists": True}})
    assert len(docs) == 3
    assert docs[1]["doc"]["exists_field"] == "should_exist2"
    docs = db.find({"exists_array":{"$exists": True}})
    assert len(docs) == 3
    assert docs[1]["doc"]["exists_array"] == ["should", "exist", "array1"]
    docs = db.find({"exists_object":{"$exists": True}})
    assert len(docs) == 3
    assert docs[1]["doc"]["exists_object"] == {"should": "object"}
    db.delete_index(_id, _name, idx_type="text")

# Test $nor, $not $ne. In mongo, negatives also returns documents that do
# not contain the field. Not sure if this is possible with Lucene
def test_text_comparison_operators():
    db = mkdb()
    analyzer = "keyword"
    _id = "comparison_operators"
    _name = "test_text_comparison_operators"
    ret = db.create_text_index(analyzer, name=_name, ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"favorites.[]": {"$not":{"$text":"Ruby"}}})
    assert len(docs) == 1
    docs = db.find({"$nor": [{"$text":"Stephanie"},
        {"$and":[{"age":{"$lte" : 0}}, {"age":{"$gte": 100}}]}]})
    assert len(docs) == 1
    docs = db.find({"$or": [{"$text":"Stephanie"}, {"age":{"$ne" : 31}}]})
    assert len(docs) == 2
    db.delete_index(_id, _name, idx_type="text")

# Test $elemMatch
def test_text_elemMatch():
    db = mkdb()
    analyzer = "keyword"
    _id = "elemMatch"
    _name = "test_text_elemMatch"
    ret = db.create_text_index(analyzer, name=_name,
        ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"@graph": {"$elemMatch":
        {"http://ibm%2Ecom/ce/ns#system_deployment":
         {"type": "uri", "value": "urn:ce:/sy/mysys"}}}})
    assert len(docs) == 2
    assert docs[1]['doc']['@graph'][0]["@id"] == "urn:ce:/sy/myapp"
    db.delete_index(_id, _name, idx_type="text")

#Test $elemMatch and $in nested
def test_text_elemMatch_nested():
    db = mkdb()
    analyzer = "keyword"
    _id = "elemMatch"
    _name = "test_text_elemMatch"
    ret = db.create_text_index(analyzer, name=_name, ddoc=_id)
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != _name:
            continue
        assert idx["type"] == "text"
    docs = db.find({"$and": [{"@graph": {"$elemMatch":
        {"@id": "urn:ce:/","http://ibm%2Ecom/ce/ns#sites":
        {"$exists": True}}}},{"@graph": {"$elemMatch": {"$or":
        [{"http://ibm%2Ecom/ce/ns#owner": {"type": "uri", "value":
        "urn:ce:/account/frankb#owner"}},
        {"http://ibm%2Ecom/ce/ac/ns#resource-group": {"$in":
        [{"type": "uri", "value": "urn:ce:/"},{"type": "uri", "value":
        "urn:ce:/mt/sites"}, {"type": "uri", "value":
        "urn:ce:/account"}]}}]}}}]})
    assert len(docs) == 2
    assert docs[1]['doc']['@graph'][0]["@id"] == "urn:ce:/mt/pepsi"
    db.delete_index(_id, _name, idx_type="text")
