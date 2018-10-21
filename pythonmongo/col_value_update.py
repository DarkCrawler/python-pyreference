import pymongo

connection = pymongo.MongoClient("localhost", 27017)
_db_handle = connection.m101

_db_collection = _db_handle.pi

# find max length  of outboundSeasons | outboundBlackouts | inboundSeasons | inboundBlackouts
max_len_outboundSeasons = 0
max_len_outboundBlackouts = 0
max_len_inboundSeasons = 0
max_len_inboundBlackouts = 0


def find_max_len():
    print "************** FINDING MAX LENGTH ************************"
    global max_len_outboundSeasons
    global max_len_outboundBlackouts
    global max_len_inboundSeasons
    global max_len_inboundBlackouts

    db_find_cursor = _db_collection.find({})

    for index, doc in enumerate(db_find_cursor):
        tem_len_outboundSeasons = len(doc['outboundSeasons'])
        tem_len_outboundBlackouts = len(doc['outboundBlackouts'])
        tem_len_inboundSeasons = len(doc['inboundSeasons'])
        tem_len_inboundBlackouts = len(doc['inboundBlackouts'])

        if index == 0:
            max_len_outboundSeasons = tem_len_outboundSeasons
            max_len_outboundBlackouts = tem_len_outboundBlackouts
            max_len_inboundSeasons = tem_len_inboundSeasons
            max_len_inboundBlackouts = tem_len_inboundBlackouts

        else:
            if max_len_outboundSeasons < tem_len_outboundSeasons:
                max_len_outboundSeasons = tem_len_outboundSeasons
            if max_len_outboundBlackouts < tem_len_outboundBlackouts:
                max_len_outboundBlackouts = tem_len_outboundBlackouts
            if max_len_inboundSeasons < tem_len_inboundSeasons:
                max_len_inboundSeasons = tem_len_inboundSeasons
            if max_len_inboundBlackouts < tem_len_inboundBlackouts:
                max_len_inboundBlackouts = tem_len_inboundBlackouts

    print "Finding max lengths of fields ::: outboundSeasons | outboundBlackouts | inboundSeasons | inboundBlackouts"
    print "max_len_outboundSeasons == ", max_len_outboundSeasons
    print "max_len_outboundBlackouts ==", max_len_outboundBlackouts
    print "max_len_inboundSeasons ==", max_len_inboundSeasons
    print "max_len_inboundBlackouts ==", max_len_inboundBlackouts


# Update rows to
def do_update_magic():
    print "*********************** STARTING UPDATE MAGIC **********************"
    db_find_cursor = _db_collection.find({})
    for data in db_find_cursor:
        _idvar = data['_id']
        tem_outboundSeasons = data['outboundSeasons']
        tem_outboundBlackouts = data['outboundBlackouts']
        tem_inboundSeasons = data['inboundSeasons']
        tem_inboundBlackouts = data['inboundBlackouts']

        if abs(len(tem_outboundSeasons) - max_len_outboundSeasons) > 0 \
                or abs(len(tem_outboundBlackouts) - max_len_outboundBlackouts) > 0 \
                or abs(len(tem_inboundSeasons) - max_len_inboundSeasons) > 0 \
                or abs(len(tem_inboundBlackouts) - max_len_inboundBlackouts) > 0:
            insert_docs(tem_outboundSeasons, max_len_outboundSeasons)
            insert_docs(tem_outboundBlackouts, max_len_outboundBlackouts)
            insert_docs(tem_inboundSeasons, max_len_inboundSeasons)
            insert_docs(tem_inboundBlackouts, max_len_inboundBlackouts)
            _db_collection.replace_one({'_id': _idvar}, data)
            print "_id:::", _idvar, "..replaced..."


def insert_docs(doc, maxlenofdoc):
    difflen = maxlenofdoc - len(doc)
    if difflen > 0:
        for x in range(0, difflen):
            doc.append({"begin": "null", "end": "null"})


find_max_len()
do_update_magic()
