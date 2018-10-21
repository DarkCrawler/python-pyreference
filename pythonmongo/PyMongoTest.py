from pymongo import MongoClient

connection = MongoClient('localhost', 27017)
db = connection.m101

scores_db = db.scores


def _findcount(collection):
    score_count = collection.count()
    print score_count


def _findquery(collection, query):
    try:
        cursor = collection.find(query)

    except Exception as e:
        print  "Unexpected Error :::", type(e), e

    index = 0
    for doc in cursor:
        print doc
        index += 1
        if index > 10:
            break


def _cursorbuilderfind(collection, query):
    try:
        cursor = collection.find(query)
    except Exception as e:
        print "Unexpected error :::", type(e), e

    return cursor


def _cursourbuilderfindone(collection, query):
    try:
        cursor = collection.find_one(query)
    except Exception as e:
        print "Unexpected error ::", type(e), e

    return cursor


def _findone(collection, query):
    findonecursor = _cursourbuilderfindone(collection, query)
    print findonecursor


def _projecion(collection):
    query = {'type': 'exam'}
    projectionquery = {'student_id': 1, '_id': 0}



# _findcount(scores_db)
_queryforscores = {'type': 'exam'}
_findone(scores_db, _queryforscores)
# _findquery(scores_db, _queryforscores)
