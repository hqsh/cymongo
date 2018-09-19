from cymongo import CyMongoClient
import time

mongo_uri = 'mongodb://hqs:f@localhost:27017'
client = CyMongoClient(mongo_uri)

# collection = client['test']['cymongo']
collection = client['test']['cymongo_1000_4000']

# return_type = 'table'
return_type = 'data_frame'

collection.enable_debug()

if return_type == 'table':
    collection.set_table_info(['date_time', 'blogger_id', 'daily_visitor_cnt', 'fan_cnt', # 'signature',
                               'last_logged_date_time', 'heat_degree', 'blog_updated'])
    begin = time.time()
    df = collection.find(return_type='table')
    print('find as table by cymongo cost: {}'.format(time.time() - begin))
    df.set_index(['date_time', 'blogger_id'], inplace=True)
    print('set_index cost: {}'.format(time.time() - begin))
    df = df.unstack('blogger_id')
    print('unstack cost: {}'.format(time.time() - begin))
    print(df)
elif return_type == 'data_frame':
    collection.set_data_frame_info('date_time', 'blogger_id', [  # 'signature',
        'daily_visitor_cnt', 'fan_cnt', 'last_logged_date_time', 'heat_degree', 'blog_updated'])
    begin = time.time()
    dfs = collection.find(return_type='data_frame')
    print('find as DataFrames by cymongo cost: {}'.format(time.time() - begin))
    for df_name, df in dfs.items():
        print('================================== {} =================================='.format(df_name))
        print(df.columns.shape, df.columns.tolist())

