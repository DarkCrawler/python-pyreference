import pymongo


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


def _cursorbuilderprojection(collection, findquery, projectionquery):
    try:
        projectioncursor = collection.find(findquery, projectionquery)
    except Exception as e:
        print "Unexpected error ::", type(e), e

    return projectioncursor


def _cursorprinter(cursor, index):
    if index == -1:
        for data in cursor:
            print data
    else:
        _innerindex = 0
        for data in cursor:
            print data
            _innerindex += 1
            if _innerindex == index:
                break


def _cursorsortskiplimit(collection, query, sortcol, skip, limit):
    try:
        return collection.find(query).sort(sortcol, pymongo.ASCENDING).skip(skip).limit(limit)
    except Exception as e:
        print "Exception -->", type(e), e


class pymongoutil:
    def __init__(self):
        pass
