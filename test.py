from cymongo import CyMongoClient

mongo_uri = 'mongodb://hqs:f@localhost:27017'
client = CyMongoClient(mongo_uri)

# collection = client['test']['cymongo_instrument']
# collection.set_data_frame_info('trade_date', 'instrument_id', ['high', 'low', 'exchange_code'])

# collection = client['test']['cymongo']
# collection.set_data_frame_info('date_time', 'blogger_id', [
#     'signature', 'daily_visitor_cnt', 'fan_cnt', 'last_logged_date_time', 'heat_degree', 'blog_updated'])

collection = client['test']['cymongo_1000_4000']
# collection = client['test']['cymongo']
collection.set_table_info(['date_time', 'blogger_id', 'signature', 'daily_visitor_cnt', 'fan_cnt',
                           'last_logged_date_time', 'heat_degree', 'blog_updated'])


# collection.enable_debug()
dfs = collection.find()
# for df_name, df in dfs.items():
#     print('================================== {} =================================='.format(df_name))
#     print(df.info())


