from pymongo import MongoClient
from collections import OrderedDict
import datetime
import random

client = MongoClient('mongodb://hqs:f@localhost:27017/test')
db = client['test']
collection = db['cymongo']

# row_cnt = 100
# col_cnt = 4000
row_cnt = 10
col_cnt = 5



start_date_time = datetime.datetime(2010, 1, 1)
signatures = [
    '这家伙很懒',
    'Just do it!',
    '666',
    '悱炷蓅'
]

for delta_day in range(row_cnt):
    docs = []
    for blogger_id in range(1, col_cnt + 1):
        date_time = start_date_time + datetime.timedelta(days=delta_day)
        doc = OrderedDict()
        doc['date_time'] = date_time                                                               # index
        doc['blogger_id'] = blogger_id                                                             # column
        doc['signature'] = signatures[random.randint(0, len(signatures)-1)]                        # string value
        # doc['daily_visitor_cnt'] = random.randint(1000, 100000)                                    # int32 value
        # doc['fan_cnt'] = random.randint(10000000000, 100000000000)                                 # int64 value
        # doc['last_logged_date_time'] = date_time - datetime.timedelta(days=random.randint(0, 10))  # date time value
        # doc['heat_degree'] = random.random() // 0.0001 / 100                                       # float64 value
        # doc['blog_updated'] = random.randint(0, 1) == 1                                            # bool value
        doc['daily_visitor_cnt'] = 1000 * blogger_id
        doc['fan_cnt'] = 100000000000 * blogger_id
        doc['last_logged_date_time'] = date_time
        doc['heat_degree'] = 100.0 * blogger_id
        doc['blog_updated'] = blogger_id % 2 == 0
        docs.append(doc)
    collection.insert_many(docs)
    if delta_day % 10 == 0:
        print('{} rows inserted'.format(delta_day + 10))
