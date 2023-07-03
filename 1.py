import datetime
import tushare as ts
import pymysql
import pandas

###############################
#stock_basic
ts.set_token('a9a4dd034b7de7f9c29014b05ee7d5295ea546fe23ace4ce84c13d26')
pro = ts.pro_api()
#查询当前所有正常上市交易的股票列表
#data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

data = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20210505')
###############################
#print(data)
#提取2018年7月18日复权因子

#提取000001全部复权因子
df1 = pro.adj_factor(ts_code='000001.SZ', start_date='20180701', end_date='20210505')
df = pro.daily_basic(ts_code='000001.SZ',start_date='20180701', end_date='20210505' , fields='ts_code,trade_date,turnover_rate_f,volume_ratio,pe,pb')
print(df)
data.insert(11, 'adj_factor', df1['adj_factor'].values)
data.insert(12, 'turnover_rate_f', df['turnover_rate_f'].values)
data.insert(13, 'volume_ratio', df['volume_ratio'].values)
data.insert(14, 'pe', df['pe'].values)
data.insert(15, 'pb', df['pb'].values)
print(data)