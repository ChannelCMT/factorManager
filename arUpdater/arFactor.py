# -*- coding: utf-8 -*-
import sys 
sys.path.append("..") 
import readDataTool
from vector import portfolio, data_source
import importlib
from datetime import datetime, timedelta, timezone
import os
import json
import tables as tb
import pandas as pd
import talib as ta
from sklearn.decomposition import PCA
import numpy as np
import time
import pymongo
# get original data
importlib.reload(data_source)
import warnings
warnings.filterwarnings("ignore")

class Signal(data_source.DataManager):
    def cal_pca(self, pctArray, paramDict):
        num = len(paramDict['symbols'])
        pca = PCA(n_components=num)
        pca.fit(pctArray)
        res = pca.explained_variance_ratio_[0]
        return pca.explained_variance_ratio_[0]

    def cal_ar(self,data_raw, ar_param, paramDict):
        symbolsPct = data_raw.loc[:, pd.IndexSlice[:, "close"]].pct_change().dropna()
        pcaList = []
        for i in range(len(symbolsPct)-ar_param+1):
            pctArray = np.array(symbolsPct.iloc[i:i+ar_param])
            pcaList.append(self.cal_pca(pctArray, paramDict))
        
        symbolsPca = symbolsPct.iloc[ar_param-1:]
        symbolsPca['absorption'] = pcaList
        data_raw["ar"+str(ar_param)] = symbolsPca['absorption']
        colname = data_raw.columns.tolist()[-1]
        print(f'{colname[0]} done')
        return data_raw

    def cal_symbols_ar(self, paramDict):
        data = self.basic_data.copy(deep=True)
        ar_param = paramDict['ar_param']
        for ar_parameter in ar_param:
            data = self.cal_ar(data, ar_parameter, paramDict)
        res = data.iloc[:,-len(ar_param):].dropna(how='all',axis=0)
        return res

if __name__ == '__main__':
    jsonName = 'arParam5Min.json'
    with open(jsonName) as param:
        paramDict = json.load(param)
    dataToolClass = readDataTool.DataTool(paramDict, './'+jsonName)
    symbolsDataDict = dataToolClass.get_data('vector_cache')
    arRes = Signal(symbolsDataDict).cal_symbols_ar(paramDict)
    dataToolClass.upload_data(arRes, paramDict['factorName'], paramDict['symbols'])