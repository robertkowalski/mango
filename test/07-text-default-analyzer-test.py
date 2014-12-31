
import mango


# # # Verifies the default analyzer is keyword
# def test_text_index_default_analyzer():
#     db = mkdb()
#     #analyzer = "email"
#     _id = "index_default_analyzer"
#     _name = "test_text_index_default_analyzer"
#     ret = db.create_text_index(name=_name, ddoc=_id)
#     assert ret is True
#     for idx in db.list_indexes():
#         if idx["name"] != _name:
#             continue
#         assert idx["type"] == "text"
#     docs = db.find({"email":{"$text":"dreamia.com"}})
#     assert len(docs) == 1
#     docs = db.find({"email":{"$text":"stephaniekirkland@dreamia.com"}})
#     assert len(docs) == 2
#     assert docs[1]["doc"]["email"] == "stephaniekirkland@dreamia.com"
#     db.delete_index(idx["ddoc"], idx["name"], idx_type="text")
#