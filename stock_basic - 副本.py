import datetime
import tushare as ts
import pymysql
def creat_database(table_name):
    # deal new
    # 开始建库 stock_daily 存储所有的上市A股 日K线数据
    con = pymysql.connect(host='localhost', user='root',
                          passwd='alpeko123', charset='utf8')
    cur = con.cursor()
    # 使用库
    cur.execute("use stock_daily;")
    ##############
    print(table_name)
    # 建表ts_code     trade_date  open  high   low  close  pre_close  change    pct_chg  vol   amount
    cur.execute("create table `%s` (ts_code char(20), trade_date char(8), open float(5,2), high float(5,2), low float(5,2), close float(5,2), pre_close float(5,2), changes float(5,2), pct_chg float(10,5), vol float(15,3), amount float(14,3))character set utf8;" %(table_name))

   # cur.execute(
   #     "create table `%s`(ts_code char(20), trade_date int(4), open float(5,2), high float(5,2), low float(5,2), close float(5,2), pre_close float(5,2), change float(5,2), pct_chg float(5,2), vol float(12,2), amount float(15,2))character set utf8" %(table_name))

    #########################
    #cur.close()
    #db.close()  # 关闭连接

def deal_stock_basic(resu0):
    resu = []
    for k in range(len(resu0)):
        if str(resu0[k]) == 'nan':
            resu.append(-1)
        else:
            resu.append(resu0[k])
    return resu
def save_stock_basic(resu, table_name):
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='alpeko123', db='stock_daily', charset='utf8')
    # 获取游标
    cur = db.cursor()
    sql = "INSERT INTO `%s` (ts_code, trade_date, open, high, low, close, pre_close, changes, pct_chg, vol, amount)" % (
        table_name) + 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    try:
    # 执行sql
        #print(resu)
        #print('插入成功')
        cur.execute(sql,
                (resu[0], resu[1], float(resu[2]), float(resu[3]), float(resu[4]), float(resu[5]), float(resu[6]), float(resu[7]), float(resu[8]), float(resu[9]), float(resu[10])))
        db.commit()
    except Exception as e:
        print(e)
        db.rollback()
        print(resu)
        print('插入失败')
    finally:
        cur.close()
        db.close()  # 关闭连接
        #print(resu)
###############################
#query ts_code, symbol and list_data
db = pymysql.connect(host='127.0.0.1', user='root', passwd='alpeko123', db='All_stock', charset='utf8')
# 获取游标
cur = db.cursor()
sql = 'select * from stuck_list'
try:
        cur.execute(sql)  # 执行sql语句
        results = cur.fetchall()  # 获取查询的所有记录
except Exception as e:
        raise e
finally:
        cur.close()
        db.close()  # 关闭连接
ts_code = list()
symbol = list()
list_data = list()
#print(results)
for row in results:
    ts_code.append(str(row[0]))
    symbol.append(str(row[1]))
    list_data.append(str(row[5]))
###############################
#current date
time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
yesterday = time_temp.strftime('%Y%m%d')
####################################3
# deal new
# 开始建库 stock_daily 存储所有的上市A股 日K线数据
con = pymysql.connect(host='localhost', user='root',
                      passwd='alpeko123', charset='utf8')
cur = con.cursor()
cur.execute("create database stock_daily character set utf8;")
# 使用库
cur.execute("use stock_daily;")

for i in range(len(ts_code)):
    creat_database(symbol[i])
    if int(((int(yesterday) - int(list_data[i]))/100000)) == 0:
        pro = ts.pro_api()
        df = pro.daily(ts_code= ts_code[i], start_date=list_data[i], end_date=yesterday)
        for k in range(df.shape[0]):
            resu0 = list(df.iloc[df.shape[0] - 1 - k])
            resu = deal_stock_basic(resu0)
            save_stock_basic(resu, symbol[i])

    else:
        star_day = int(list_data[i])
        for j in range(int(((int(yesterday) - int(list_data[i]))/100000))):
            print("时间")
            end_day = star_day + 100000
            print('%d %d %d' %(star_day,end_day, int(yesterday)))
            pro = ts.pro_api()
            df = pro.daily(ts_code=ts_code[i], start_date=star_day, end_date=end_day)
            for k in range(df.shape[0]):
                resu0 = list(df.iloc[df.shape[0] - 1 - k])
                resu = deal_stock_basic(resu0)
                save_stock_basic(resu, symbol[i])
            star_day = end_day
        pro = ts.pro_api()
        df = pro.daily(ts_code=ts_code[i], start_date=star_day, end_date=yesterday)
        for k in range(df.shape[0]):
            resu0 = list(df.iloc[df.shape[0] - 1 - k])
            resu = deal_stock_basic(resu0)
            save_stock_basic(resu, symbol[i])
    #pro = ts.pro_api()
    #df = pro.daily(ts_code= ts_code[i], start_date=list_data[i], end_date=yesterday)
    #print(ts_code[i])
    #print(list_data[i])
    #print(df)


