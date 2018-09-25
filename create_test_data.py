from pymongo import MongoClient
from collections import OrderedDict
import datetime
import random


if __name__ == '__main__':
    start_date_time = datetime.datetime(2010, 1, 1)
    signatures = [
        '这家伙很懒',
        'Just do it!',
        '666',
        '悱炷蓅'
    ]
    client = MongoClient('mongodb://hqs:f@localhost:27017/test')
    db = client['test']
    collections = {'cymongo': (10, 5), 'cymongo_1000_4000': (1000, 4000), 'cymongo_nan': (3, 3)}
    for collection_name, (row_cnt, col_cnt) in collections.items():
        print('create collection "{}" start.'.format(collection_name))
        collection = db[collection_name]
        for delta_day in range(row_cnt):
            docs = []
            for blogger_id in range(1, col_cnt + 1):
                if 'nan' in collection_name and delta_day == 0 and blogger_id == 1:
                    continue
                str_blogger_id = str(blogger_id)
                date_time = start_date_time + datetime.timedelta(days=delta_day)
                doc = OrderedDict()
                doc['date_time'] = date_time                                                                 # index: date time
                # doc['blogger_id'] = blogger_id
                doc['blogger_id'] = '{}{}'.format('0' * (11 - len(str_blogger_id)), str_blogger_id)          # column: string
                if not ('nan' in collection_name and delta_day == 0 and blogger_id == 2):
                    doc['signature'] = signatures[random.randint(0, len(signatures)-1)]                          # string value
                    # doc['daily_visitor_cnt'] = random.randint(1000, 100000)                                    # int32 value
                    # doc['fan_cnt'] = random.randint(10000000000, 100000000000)                                 # int64 value
                    # doc['last_logged_date_time'] = date_time - datetime.timedelta(days=random.randint(0, 10))  # date time value
                    # doc['heat_degree'] = random.random() // 0.0001 / 100                                       # float64 value
                    # doc['blog_updated'] = random.randint(0, 1) == 1                                            # bool value
                    doc['daily_visitor_cnt'] = 1000 * blogger_id                                                 # int32 value
                    doc['fan_cnt'] = 100000000000 * blogger_id                                                   # int64 value
                    doc['last_logged_date_time'] = date_time                                                     # date time value
                    doc['heat_degree'] = 100.0 * blogger_id                                                      # float64 value
                    doc['blog_updated'] = blogger_id % 2 == 0                                                    # bool value
                docs.append(doc)
            collection.insert_many(docs)
            if delta_day % 10 == 0:
                print('data of {} days inserted'.format(delta_day + 10))
        print('create collection "{}" end.'.format(collection_name))
