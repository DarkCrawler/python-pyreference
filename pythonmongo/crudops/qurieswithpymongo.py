from pymongo import MongoClient

from utils import pymongoutil

connection = MongoClient('localhost', 27017)
db = connection.m101

scores_collection = db.scores
reddit_collection = db.reddit


def _findone(collection, query):
    findonecursor = pymongoutil._cursourbuilderfindone(collection, query)
    print findonecursor


def _projection(collection):
    query = {'type': 'exam'}
    projectionquery = {'student_id': 1, '_id': 0}
    projectioncursor = pymongoutil._projectioncursor(collection, query, projectionquery)
    pymongoutil._cursorprinter(projectioncursor, 30)


def _ltegte(collection):
    query = {'type': 'exam', 'score': {'$gt': 50, '$lt': 70}}
    cursor = pymongoutil._cursorbuilderfind(collection, query)
    pymongoutil._cursorprinter(cursor, 20)


def _regex(collection):
    query = {'title': {'$regex': 'apple|google', '$options': 'ix'}}
    cursor = pymongoutil._cursorbuilderfind(collection, query)
    pymongoutil._cursorprinter(cursor, 100)


def _sortskiplimit(collection):
    query = {}
    sortcol = 'student_id'
    cursor = pymongoutil._cursorsortskiplimit(collection, query, sortcol, 2, 10)
    pymongoutil._cursorprinter(cursor, -1)


# _findcount(scores_db)
# _queryforscores = {'type': 'exam'}
# _findone(scores_collection, _queryforscores)
# _projecion(scores_collection)
# _ltegte(scores_collection)
# _regex(reddit_collection)
_sortskiplimit(scores_collection)
