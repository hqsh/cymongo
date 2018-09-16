from pymongo import MongoClient
import pandas as pd
import time

client = MongoClient('mongodb://hqs:f@localhost:27017/test')
db = client['test']
collection = db['cymongo_1000_4000']

data = {'date_time': [], 'blogger_id': [], 'signature': [], 'daily_visitor_cnt': [], 'fan_cnt': [], 'last_logged_date_time': [], 'heat_degree': [], 'blog_updated': []}
# data = []
begin = time.time()
cur = collection.find({})
for doc in cur:
    for value_name in ['date_time', 'blogger_id', 'signature', 'daily_visitor_cnt', 'fan_cnt', 'last_logged_date_time', 'heat_degree', 'blog_updated']:
    #     x = doc
        data[value_name].append(doc[value_name])
    # data.append(doc)
print('step 1: get data into dict from mongo: {}'.format(time.time() - begin))
df = pd.DataFrame(data)
print('step 2: create multi-columns DataFrame from dict: {}'.format(time.time() - begin))
print(df.shape)
df = df.set_index(['date_time', 'blogger_id'])
print('step 3: set_index: {}'.format(time.time() - begin))
df = df.unstack('blogger_id')
print('step 4: unstack: {}'.format(time.time() - begin))
print(df.shape)
print(df.head())
