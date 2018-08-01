from cymongo import CyMongoClient

mongo_uri = 'mongodb://hqs:f@localhost:27017'
client = CyMongoClient(mongo_uri)
collection = client['test']['cymongo']
collection.set_data_frame_info('trade_date', 'instrument_id', ['high', 'low', 'exchange_code'])
collection.enable_debug()
collection.find()
# collection.find()
