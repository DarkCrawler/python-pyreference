from pymongo import MongoClient, ReturnDocument

from util import pymongoutil
import datetime

connection = MongoClient('localhost', 27017)
db = connection.m101

scores_collection = db.scores
reddit_collection = db.reddit
people_collection = db.people
people_collection.drop()

user1 = {"name": "Khan Noonien Singh", "movie": "Wrath of Khan",
         "interests": ['ruling a civilization', 'quoting millers', 'killing the protagonist']}

user2 = {"name": "Mola Ram", "movie": "Temple of Doom",
         "interests": ['pleasing maa kaali', 'killing the protagonist']}

user3 = {"name": "Darth Vader", "movie": "Star Wars Triology",
         "interests": ['increasing the force', 'killing the protagonist']}


def _insert_one():
    try:
        people_collection.insert_one(user1)
        people_collection.insert_one(user2)
        people_collection.insert_one(user3)

    except Exception as e:
        print "Exception -->", type(e), e

    print("all users inserted")


def _insert_many_ordered():
    bulk_user_to_insert = [user1, user2, user3]
    try:
        people_collection.insert_many(bulk_user_to_insert, ordered=True)
    except Exception as e:
        print "Error is...", e

    findcursor = pymongoutil._cursorbuilderfind(people_collection, {})
    pymongoutil._cursorprinter(findcursor, 10)


def _update_one(collection, student_id):
    try:
        findquery = {'student_id': student_id, 'type': 'homework'}
        findcursor = pymongoutil._cursourbuilderfindone(collection, findquery)
        _id_ = findcursor['_id']
        print "fetching students details"
        pymongoutil._cursorprinter(findcursor, -1)

        print "updating the record now"
        result = collection.update_one({'_id': _id_}, {'$set': {'score': datetime.datetime.utcnow()}})
        # change function to update_many
        # for upsert::
        # collection.update_one({'_id': _id_}, {'$set': {'score': datetime.datetime.utcnow()}},upsert = True)
        print result.matched_count

        print "printing new details"
        findquery = {'student_id': student_id, 'type': 'homework'}
        findcursor = pymongoutil._cursorbuilderfind(collection, findquery)
        print "fetching students details"
        pymongoutil._cursorprinter(findcursor, -1)

    except Exception as e:
        print "Error...", type(e), e


def _replace_one(collection, student_id):
    # used to replace and update the entire document
    try:
        find_cursor = pymongoutil._cursourbuilderfindone(collection, {'student_id': student_id})
        print "fetched document::"
        print find_cursor
        find_cursor['score'] = 90000

        _fetched_id = find_cursor['_id']
        print "doing replace one"
        collection.replace_one({'_id': _fetched_id}, find_cursor)

        find_cursor = pymongoutil._cursourbuilderfindone(collection, {'student_id': student_id})
        print "post replace"
        print find_cursor

    except Exception as e:
        print "exception:::", type(e), e
        raise


def _find_and_modify(collection, studentid):
    try:
        _cursor = collection.find_one_and_update(filter={'student_id': studentid}, update={'$inc': {'score': 1000}},
                                                 upsert=True,
                                                 return_document=ReturnDocument.AFTER)

    except Exception as e:
        print "Exception -->", type(e), e

    print "==>incremented score for ", studentid, " == ", _cursor['score']
    # print "count value==", _cursor['value']


# _insert_one()
# _insert_many_ordered()
# _update_one(scores_collection, 1)
# _replace_one(scores_collection, 1)
_find_and_modify(scores_collection, 8888)
