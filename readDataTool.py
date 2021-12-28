# -*- coding: utf-8 -*-
import sys 
sys.path.append("..") 
from vector import portfolio, data_source
import importlib
from datetime import datetime, timedelta, timezone
import pickle
import os
import json
import tables as tb
import pandas as pd
import talib as ta
import numpy as np
import time
import pymongo
# get original data
importlib.reload(data_source)
import warnings
warnings.filterwarnings("ignore")

class DataSource(data_source.SourceManager):
    
    # 定义转换数据源命名转换规则: eth -> eth_usdt.spot:binance (MongoDB表名)
    def source_key_map(self, key: str):
        return f"{key}_usdt.spot:binance"
    
    # 定义本地缓存文件命名规则：eth -> eth
    def target_key_map(self, key: str):
        return key

class DataTool():
    def __init__(self, setting, jsonPath):
        '''
        实例化时需要传入参数版本和备注，之后保存数据和读入数据都按照此参数版本和备注进行，无需再另外设置
        '''
        self.setting = setting
        print(setting)
        self.jsonPath = jsonPath
        self.symbolsData = pd.DataFrame()
        self.MONGODB_HOST = self.setting['MONGODB_HOST']
        self.KLINE_DB = self.setting['KLINE_DB']
        self.symbols = self.setting['symbols']
        self.freqs = self.setting['sigPeriod']
        self.dictDf = {}
        if self.setting['firstTimeRun']:
            self.begin_time = datetime(2018,6,1).timestamp()
        else:
            self.begin_time = self.setting['lastUpdateTime']
        self.end_time = datetime.now().timestamp()
        print(f'DataTool.__init__() is called')
        print('symbols:',self.symbols)
        print('freqs:', self.freqs)

    def get_data(self, dsPath):
        '''
        取原始数据并合成K线
        :begin_time :从数据库取数据的初始时间 数据例为1630928800.0的毫秒格式
        :end_time :从数据库取数据的结束时间
        '''
        ds = DataSource.from_mongodb(
            self.MONGODB_HOST,
            self.KLINE_DB,
            root = '../%s'%(dsPath) #存放缓存的默认位置
        )
        # 从数据库拉取一分钟数据
        print('从数据库拉取',self.symbols,'的数据')
        ds.pull(self.symbols, begin=self.begin_time, end=self.end_time)
        # 合成不同周期k线
        print(self.symbols, self.freqs)
        print('DataTool.get_data() is called')
        ds.resample(self.symbols, self.freqs)
        result = ds.load(self.symbols, self.freqs, self.begin_time, self.end_time) #result类型是dict
        self.dictDf = result   #函数到这行为止是Channel的代码
        self.symbolsData = result[self.freqs[0]]
        return self.dictDf

    def update_json(self, last_ind):
        '''
        返回的lastIndex是timestamp格式 
        '''
        with open(self.jsonPath, 'r') as pastParam:
            json_dict = json.load(pastParam)
            json_dict['lastUpdateTime'] = last_ind
        pastParam.close()
        with open(self.jsonPath, 'w') as update:
            json.dump(json_dict, update)
        update.close()
        print('jsonUpdated:', last_ind)
        
    def upload_data(self,df:pd.DataFrame(), factor:str, symbols:list):
        '''
        把数据传到数据库
        df是以timestamp为索引的dataframe，需要reset_index()
        factor应传入因子名字符串，如absorptionRatio
        '''
        client = pymongo.MongoClient('172.16.20.81', 27017)
        collection = client['multiSymbolsFactor'][factor]
        df = df.reset_index()
        # print('df###############:', df)
        link = ','
        strRes = link.join(symbols)
        symbolList = [strRes]*len(df)
        df['symbols'] = symbolList
        dfCopy = df.copy(deep=True)
        dfCopy.columns = [tu[0] for tu in df.columns.tolist()]
        for index, values in dfCopy[:].iterrows():
            bar = values.to_dict()
            bar['datetime'] = values['timestamp']+timedelta(hours=8)
            del bar['timestamp']
            collection.create_index([('datetime', pymongo.ASCENDING)], unique=True)
            flt = {'datetime': bar['datetime']}
            collection.update_one(flt, {'$set':bar}, upsert=True)
            print(index,' write complete')
        last_ind = int(df['timestamp'].iloc[-100].timestamp())
        print('last_ind:', last_ind)
        self.update_json(last_ind)
        num = str(len(df))
        print(f'Upload {num} {factor} data complete')