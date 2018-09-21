from cymongo import CyMongo, CyMongoClient
from pymongo import MongoClient
from logger import with_logger
import datetime
import numpy as np
import pandas as pd
import time
import unittest


@with_logger
class CymongoTest(unittest.TestCase):
    mongo_uri = 'mongodb://hqs:f@localhost:27017/test'
    db = 'test'
    enable_debug = True
    index_name = 'date_time'
    column_name = 'blogger_id'
    value_names = [  # 'signature', 'last_logged_date_time'
        'daily_visitor_cnt', 'fan_cnt', 'heat_degree', 'blog_updated']
    offset_hour = 8  # GMT+8
    keep_int_when_has_nan_value = True
    default_int32_nan_value = - 2 ** 31
    default_int64_nan_value = - 2 ** 63
    default_date_time_nan_value = 0
    default_bool_value = False

    pymongo_find_need_fillna_astype = False
    value_dtypes = {'daily_visitor_cnt': np.int64, 'fan_cnt': np.int64, 'last_logged_date_time': np.int64,
                    'blog_updated': np.bool}
    default_nan_values = {'daily_visitor_cnt': default_int64_nan_value, 'fan_cnt': default_int64_nan_value,
                          'last_logged_date_time': default_date_time_nan_value, 'blog_updated': default_bool_value}

    def pymongo_find(self):
        self.logger.info('--------------------------------------- pymongo_find ---------------------------------------')
        client = MongoClient(self.mongo_uri)
        collection = client[self.db][self.collection]
        data = {name: [] for name in [self.index_name] + [self.column_name] + self.value_names}
        begin = time.time()
        cur = collection.find(self.pymongo_filter)
        for doc in cur:
            for value_name in data.keys():
                if value_name in doc:
                    value = doc[value_name]
                    if isinstance(value, datetime.datetime):
                        value = value.year * 10000000000000 + value.month * 100000000000 + value.day * 1000000000 + \
                                value.hour * 10000000 + value.minute * 100000 + value.second * 1000 + value.microsecond
                    data[value_name].append(value)
                else:
                    if value_name == 'blog_updated':
                        data[value_name].append(False)
                    else:
                        data[value_name].append(np.nan)
        self.logger.info('step 1: get data into dict from mongo: {}'.format(time.time() - begin))
        table = pd.DataFrame(data)
        self.logger.info('step 2: create DataFrame from dict: {}'.format(time.time() - begin))
        df = table.set_index(['date_time', 'blogger_id'])
        self.logger.info('step 3: set_index: {}'.format(time.time() - begin))
        df = df.unstack('blogger_id')
        self.logger.info('step 4: unstack: {}'.format(time.time() - begin))
        if self.pymongo_find_need_fillna_astype:
            for col_name in table.columns:
                if col_name in self.value_dtypes:
                    # fillna and astype for table
                    if table[col_name].values.dtype != self.value_dtypes[col_name]:
                        arr = table[col_name].values.copy()
                        arr[np.isnan(arr)] = self.default_nan_values[col_name]
                        arr = arr.astype(self.value_dtypes[col_name])
                        table[col_name] = arr
                    # fillna and astype for df
                    if df[col_name].values.dtype != self.value_dtypes[col_name]:
                        arr = df[col_name].values.copy()
                        if self.value_dtypes[col_name] is np.bool:
                            arr[arr == None] = self.default_nan_values[col_name]
                        else:
                            arr[np.isnan(arr)] = self.default_nan_values[col_name]
                        arr = arr.astype(self.value_dtypes[col_name])
                        df[col_name] = arr
        return table, df

    def cymongo_find_as_table(self):
        self.logger.info('----------------------------------- cymongo_find_as_table ----------------------------------')
        client = CyMongoClient(self.mongo_uri)
        collection = client[self.db][self.collection]
        collection.set_nan_process_method(
                self.keep_int_when_has_nan_value, self.default_int32_nan_value, self.default_int64_nan_value,
                self.default_date_time_nan_value, self.default_bool_value)
        if self.enable_debug:
            collection.enable_debug()
        collection.set_table_info([self.index_name] + [self.column_name] + self.value_names)
        begin = time.time()
        table = collection.find(self.cymongo_filter, return_type='table')
        self.logger.info('find as table by cymongo cost: {}'.format(time.time() - begin))
        df = table.set_index(['date_time', 'blogger_id'])
        self.logger.info('set_index cost: {}'.format(time.time() - begin))
        df = df.unstack('blogger_id')
        self.logger.info('unstack cost: {}'.format(time.time() - begin))
        return table, df

    def cymongo_find_as_data_frame(self):
        self.logger.info('-------------------------------- cymongo_find_as_data_frame --------------------------------')
        client = CyMongoClient(self.mongo_uri)
        collection = client[self.db][self.collection]
        collection.set_nan_process_method(
                self.keep_int_when_has_nan_value, self.default_int32_nan_value, self.default_int64_nan_value,
                self.default_date_time_nan_value, self.default_bool_value)
        if self.enable_debug:
            collection.enable_debug()
        collection.set_data_frame_info(self.index_name, self.column_name, self.value_names)
        begin = time.time()
        dfs = collection.find(self.cymongo_filter, return_type='data_frame')
        self.logger.info('find as DataFrames by cymongo cost: {}'.format(time.time() - begin))
        return dfs

    def assert_table_equal(self, table1, table2):
        table1 = table1.sort_index(axis=1)
        table2 = table2.sort_index(axis=1)
        self.assertEqual(table1.shape, table2.shape)
        self.assertEqual((np.array(sorted(table1.columns.tolist())) == np.array(sorted(table2.columns.tolist()))).sum(),
                         table1.columns.size)
        for column_name in table1.columns:
            self.assertEqual(table1[column_name].dtype, table2[column_name].dtype)
            test_equal_cnt = (table1[column_name].values == table2[column_name].values).sum()
            try:
                correct_equal_cnt = (~np.isnan(table1[column_name].values)).sum()
            except TypeError:
                correct_equal_cnt = table1.shape[0]
            self.assertEqual(test_equal_cnt, correct_equal_cnt)

    def assert_data_frame_equal(self, df1, df2):
        self.assertEqual(df1.index.name, df2.index.name)
        self.assertEqual(df1.columns.name, df2.columns.name)
        self.assertEqual(df1.shape, df2.shape)
        self.assertEqual(df1.index.dtype, df2.index.dtype)
        self.assertEqual(df1.columns.dtype, df2.columns.dtype)
        self.assertEqual(df1.values.dtype, df2.values.dtype)
        self.assertEqual((df1.index.values == df2.index.values).sum(), df1.index.size)
        self.assertEqual((df1.columns.values == df2.columns.values).sum(), df1.columns.size)
        test_equal_cnt = (df1.values == df2.values).sum()
        try:
            correct_equal_cnt = (~np.isnan(df1.values)).sum()
        except TypeError:
            correct_equal_cnt = df1.size
        self.assertEqual(test_equal_cnt, correct_equal_cnt)

    def run_test(self):
        pymongo_table, pymongo_df = self.pymongo_find()
        cymongo_table, _ = self.cymongo_find_as_table()
        cymongo_dfs = self.cymongo_find_as_data_frame()
        self.assert_table_equal(pymongo_table, cymongo_table)
        for df_name, df in cymongo_dfs.items():
            self.assert_data_frame_equal(pymongo_df[df_name], df)

    def test_find_compare_performance(self):
        self.logger.info('=============================== test_compare_find_performance ==============================')
        self.collection = 'cymongo_1000_4000'
        self.pymongo_filter = self.cymongo_filter = {}
        self.run_test()

    def test_find_slice_col(self):
        self.logger.info('====================================== test_slice_col ======================================')
        self.collection = 'cymongo'
        self.pymongo_filter = self.cymongo_filter = {"blogger_id": {"$in": ["00000000001", "00000004000"]}}
        self.run_test()

    def test_find_slice_row(self):
        self.logger.info('====================================== test_slice_row ======================================')
        self.collection = 'cymongo'
        start_date = datetime.datetime(2010, 1, 2)
        end_date = datetime.datetime(2012, 9, 25)
        self.pymongo_filter = {"date_time": {'$gte': start_date, '$lte': end_date}}
        self.cymongo_filter = {"date_time": {
            '$gte': CyMongo.datetime_as_json_dict(start_date, offset_hour=self.offset_hour),
            '$lte': CyMongo.datetime_as_json_dict(end_date, offset_hour=self.offset_hour)
        }}
        self.run_test()

    def test_find_with_nan_value_keep_int(self):
        self.logger.info('================================= test_find_with_nan_value =================================')
        self.pymongo_find_need_fillna_astype = True
        self.collection = 'cymongo_nan'
        self.pymongo_filter = self.cymongo_filter = {}
        self.run_test()


if __name__ == '__main__':
    unittest.main()
