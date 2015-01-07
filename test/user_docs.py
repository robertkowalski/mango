# -*- coding: utf-8 -*-
"""
Generated with http://www.json-generator.com/

With this pattern:

[
    '{{repeat(20)}}',
    {
        _id: '{{guid()}}',
        user_id: "{{index()}}",
        name: {
            first: "{{firstName()}}",
            last: "{{surname()}}"
        },
        age: "{{integer(18,90)}}",
        location: {
            state: "{{state()}}",
            city: "{{city()}}",
            address: {
                street: "{{street()}}",
                number: "{{integer(10, 10000)}}"
            }
        },
        company: "{{company()}}",
        email: "{{email()}}",
        manager: "{{bool()}}",
        twitter: function(tags) {
            if(this.manager)
                return;
            return "@" + this.email.split("@")[0];
        },
        favorites: [
            "{{repeat(2,5)}}",
            "{{random('C', 'C++', 'Python', 'Ruby', 'Erlang', 'Lisp')}}"
        ]
    }
]
"""


import copy


def setup(db, index_type="view", **kwargs):
    db.recreate()
    db.save_docs(copy.deepcopy(DOCS))
    if index_type == "view":
        add_view_indexes(db, kwargs)
    elif index_type == "text":
        add_text_indexes(db, kwargs)


def add_view_indexes(db, kwargs):
    indexes = [
        ["user_id"],
        ["name.last", "name.first"],
        ["age"],
        [
            "location.state",
            "location.city",
            "location.address.street",
            "location.address.number"
        ],
        ["company", "manager"],
        ["manager"],
        ["favorites"],
        ["favorites.3"],
        ["twitter"],
        ["type"]
    ]
    for idx in indexes:
        assert db.create_index(idx) is True


def add_text_indexes(db, kwargs):
    db.create_text_index(**kwargs)


DOCS = [
    {
        "_id": "71562648-6acb-42bc-a182-df6b1f005b09",
        "user_id": 0,
        "name": {
            "first": "Stephanie",
            "last": "Kirkland"
        },
        "age": 48,
        "location": {
            "state": "Nevada",
            "city": "Ronco",
            "address": {
                "street": "Evergreen Avenue",
                "number": 347
            }
        },
        "company": "Dreamia",
        "email": "stephaniekirkland@dreamia.com",
        "manager": False,
        "twitter": "@stephaniekirkland",
        "favorites": [
            "Ruby",
            "C",
            "Python"
        ],
        "test" : [{"a":1}, {"b":2}]
    },
    {
        "_id": "12a2800c-4fe2-45a8-8d78-c084f4e242a9",
        "user_id": 1,
        "name": {
            "first": "Abbott",
            "last": "Watson"
        },
        "age": 31,
        "location": {
            "state": "Connecticut",
            "city": "Gerber",
            "address": {
                "street": "Huntington Street",
                "number": 8987
            }
        },
        "company": "Talkola",
        "email": "abbottwatson@talkola.com",
        "manager": False,
        "twitter": "@abbottwatson",
        "favorites": [
            "Ruby",
            "Python",
            "C",
            {"Versions": {"Alpha": "Beta"}}
        ],
        "test" : [{"a":1, "b":2}]
    },
    {
        "_id": "48ca0455-8bd0-473f-9ae2-459e42e3edd1",
        "user_id": 2,
        "name": {
            "first": "Shelly",
            "last": "Ewing"
        },
        "age": 42,
        "location": {
            "state": "New Mexico",
            "city": "Thornport",
            "address": {
                "street": "Miller Avenue",
                "number": 7100
            }
        },
        "company": "Zialactic",
        "email": "shellyewing@zialactic.com",
        "manager": True,
        "favorites": [
            "Lisp",
            "Python",
            "Erlang"
        ],
        "test_in": {"val1" : 1, "val2": "val2"}
    },
    {
        "_id": "0461444c-e60a-457d-a4bb-b8d811853f21",
        "user_id": 3,
        "name": {
            "first": "Madelyn",
            "last": "Soto"
        },
        "age": 79,
        "location": {
            "state": "Utah",
            "city": "Albany",
            "address": {
                "street": "Stockholm Street",
                "number": 710
            }
        },
        "company": "Tasmania",
        "email": "madelynsoto@tasmania.com",
        "manager": True,
        "favorites": [[
                "Lisp",
                "Erlang",
                "Python"
            ],
            "Erlang",
            "C",
            "Erlang"
        ]
    },
    {
        "_id": "8e1c90c0-ac18-4832-8081-40d14325bde0",
        "user_id": 4,
        "name": {
            "first": "Nona",
            "last": "Horton"
        },
        "age": 61,
        "location": {
            "state": "Georgia",
            "city": "Corinne",
            "address": {
                "street": "Woodhull Street",
                "number": 6845
            }
        },
        "company": "Signidyne",
        "email": "nonahorton@signidyne.com",
        "manager": False,
        "twitter": "@nonahorton",
        "favorites": [
            "Lisp",
            "C",
            "Ruby",
            "Ruby"
        ]
    },
    {
        "_id": "a33d5457-741a-4dce-a217-3eab28b24e3e",
        "user_id": 5,
        "name": {
            "first": "Sheri",
            "last": "Perkins"
        },
        "age": 73,
        "location": {
            "state": "Michigan",
            "city": "Nutrioso",
            "address": {
                "street": "Bassett Avenue",
                "number": 5648
            }
        },
        "company": "Myopium",
        "email": "sheriperkins@myopium.com",
        "manager": True,
        "favorites": [
            "Lisp",
            "Lisp"
        ]
    },
    {
        "_id": "b31dad3f-ae8b-4f86-8327-dfe8770beb27",
        "user_id": 6,
        "name": {
            "first": "Tate",
            "last": "Guy"
        },
        "age": 47,
        "location": {
            "state": "Illinois",
            "city": "Helen",
            "address": {
                "street": "Schenck Court",
                "number": 7392
            }
        },
        "company": "Prosely",
        "email": "tateguy@prosely.com",
        "manager": True,
        "favorites": [
            "C",
            "Lisp",
            "Ruby",
            "C"
        ]
    },
    {
        "_id": "659d0430-b1f4-413a-a6b7-9ea1ef071325",
        "user_id": 7,
        "name": {
            "first": "Jewell",
            "last": "Stafford"
        },
        "age": 33,
        "location": {
            "state": "Iowa",
            "city": "Longbranch",
            "address": {
                "street": "Dodworth Street",
                "number": 3949
            }
        },
        "company": "Niquent",
        "email": "jewellstafford@niquent.com",
        "manager": True,
        "favorites": [
            "C",
            "C",
            "Ruby",
            "Ruby",
            "Erlang"
        ],
        "exists_field" : "should_exist1"

    },
    {
        "_id": "6c0afcf1-e57e-421d-a03d-0c0717ebf843",
        "user_id": 8,
        "name": {
            "first": "James",
            "last": "Mcdaniel"
        },
        "age": 68,
        "location": {
            "state": "Maine",
            "city": "Craig",
            "address": {
                "street": "Greene Avenue",
                "number": 8776
            }
        },
        "company": "Globoil",
        "email": "jamesmcdaniel@globoil.com",
        "manager": True,
        "favorites": None,
        "exists_field" : "should_exist2"
    },
    {
        "_id": "954272af-d5ed-4039-a5eb-8ed57e9def01",
        "user_id": 9,
        "name": {
            "first": "Ramona",
            "last": "Floyd"
        },
        "age": 22,
        "location": {
            "state": "Missouri",
            "city": "Foxworth",
            "address": {
                "street": "Lott Place",
                "number": 1697
            }
        },
        "company": "Manglo",
        "email": "ramonafloyd@manglo.com",
        "manager": True,
        "favorites": [
            "Lisp",
            "Erlang",
            "Python"
        ],
        "exists_array" : ["should", "exist", "array1"]
    },
    {
        "_id": "e900001d-bc48-48a6-9b1a-ac9a1f5d1a03",
        "user_id": 10,
        "name": {
            "first": "Charmaine",
            "last": "Mills"
        },
        "age": 43,
        "location": {
            "state": "New Hampshire",
            "city": "Kiskimere",
            "address": {
                "street": "Nostrand Avenue",
                "number": 4503
            }
        },
        "company": "Lyria",
        "email": "charmainemills@lyria.com",
        "manager": True,
        "favorites": [
            "Erlang",
            "Erlang"
        ],
        "exists_array" : ["should", "exist", "array2"]

    },
    {
        "_id": "b06aadcf-cd0f-4ca6-9f7e-2c993e48d4c4",
        "user_id": 11,
        "name": {
            "first": "Mathis",
            "last": "Hernandez"
        },
        "age": 75,
        "location": {
            "state": "Hawaii",
            "city": "Dupuyer",
            "address": {
                "street": "Bancroft Place",
                "number": 2741
            }
        },
        "company": "Affluex",
        "email": "mathishernandez@affluex.com",
        "manager": True,
        "favorites": [
            "Ruby",
            "Lisp",
            "C",
            "C++",
            "C++"
        ],
        "exists_object" : {"should": "object"}
    },
    {
        "_id": "5b61abc1-a3d3-4092-b9d7-ced90e675536",
        "user_id": 12,
        "name": {
            "first": "Patti",
            "last": "Rosales"
        },
        "age": 71,
        "location": {
            "state": "Pennsylvania",
            "city": "Juntura",
            "address": {
                "street": "Hunterfly Place",
                "number": 7683
            }
        },
        "company": "Oulu",
        "email": "pattirosales@oulu.com",
        "manager": True,
        "favorites": [
            "C",
            "Python",
            "Lisp"
        ],
        "exists_object" : {"another": "object"}
    },
    {
        "_id": "b1e70402-8add-4068-af8f-b4f3d0feb049",
        "user_id": 13,
        "name": {
            "first": "Whitley",
            "last": "Harvey"
        },
        "age": 78,
        "location": {
            "state": "Minnesota",
            "city": "Trail",
            "address": {
                "street": "Pleasant Place",
                "number": 8766
            }
        },
        "company": None,
        "email": "whitleyharvey@fangold.com",
        "manager": False,
        "twitter": "@whitleyharvey",
        "favorites": [
            "C",
            "Ruby",
            "Ruby"
        ]
    },
    {
        "_id": "c78c529f-0b07-4947-90a6-d6b7ca81da62",
        "user_id": 14,
        "«ταБЬℓσ»" : "utf-8",
        "name": {
            "first": "Faith",
            "last": "Hess"
        },
        "age": 51,
        "location": {
            "state": "North Dakota",
            "city": "Axis",
            "address": {
                "street": "Brightwater Avenue",
                "number": 1106
            }
        },
        "company": "Pharmex",
        "email": "faithhess@pharmex.com",
        "manager": True,
        "favorites": [
            "Erlang",
            "Python",
            "Lisp"
        ]
    },
    {
        "type": "complex_key",
        "title": "normal key"
    },
    {
        "type": "complex_key",
        "title": "key with dot",
        "dot.key": "dot's value",
        "none": {
            "dot": "none dot's value"
        }
    },
    {
        "type": "complex_key",
        "title": "key with peso",
        "$key": "peso",
        "deep": {
            "$key": "deep peso"
        }
    },
    {
        "type": "complex_key",
        "title": "unicode key",
        "": "apple"
    }
]
