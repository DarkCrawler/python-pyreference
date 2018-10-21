from pymongo import MongoClient, ASCENDING
from util import pymongoutil

connection = MongoClient('localhost', 27017)
db = connection.m101
collection = db.grades


def _find_and_remove_lowest_score():
    try:
        cursor = collection.find({}).sort('student_id', ASCENDING)
        _curr_student_id = -1
        _curr_doc_id = -1
        _curr_lowest_score = -1
        _cursor_count = cursor.count()
        _curr_count = 0
        for data in cursor:
            _curr_count += 1
            _db_student_id = data['student_id']
            _db_doc_id = data['_id']
            _db_lowest_score = data['score']

            if _curr_student_id != _db_student_id:
                print "student id changed: current student :", _curr_student_id, " next student :", _db_student_id, ".. lowest score ::", _curr_lowest_score
                _curr_student_id = _db_student_id
                _curr_lowest_score = _db_lowest_score
                _doc_to_be_removed = _curr_doc_id
                collection.delete_one({'_id': _doc_to_be_removed})
                print "document removed ...", _doc_to_be_removed
                _curr_doc_id = _db_doc_id

            if _curr_student_id == _db_student_id:
                if _curr_lowest_score > _db_lowest_score:
                    print "lowest score for ::", _curr_student_id, "changing to ::", _db_lowest_score
                    _curr_lowest_score = _db_lowest_score
                    _curr_doc_id = _db_doc_id

            if _curr_count == _cursor_count:
                collection.delete_one({'_id': _curr_doc_id})



    except Exception as e:
        print "error ==>", type(e), e
        raise


_find_and_remove_lowest_score()
