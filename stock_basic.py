
import tushare as ts
import pymysql


def deal_stock_basic(data):
    resu = []
    for k in range(len(resu0)):
        if str(resu0[k]) == 'nan':
            resu.append(-1)
        else:
            resu.append(resu0[k])
    return resu
def save_stock_basic(resu):
        db = pymysql.connect(host='127.0.0.1', user='root', passwd='alpeko123', db='All_stock', charset='utf8')
        # 获取游标
        cur = db.cursor()
        sql = 'INSERT INTO stuck_list(ts_code, symbol, name, area, industry, list_date) VALUES ("%s", "%s", "%s", "%s", "%s", "%s")' % (
            (resu[0], resu[1], resu[2], resu[3], resu[4], resu[5]))
        try:
            # 执行sql
            print(resu)
            print('成功')
            cur.execute(sql)
            db.commit()
        except Exception as e:
            print(e)
            db.rollback()
            print(resu)
            print('插入失败')
        finally:
            cur.close()
            db.close()  # 关闭连接
###############################
#stock_basic
ts.set_token('a9a4dd034b7de7f9c29014b05ee7d5295ea546fe23ace4ce84c13d26')
pro = ts.pro_api()
#查询当前所有正常上市交易的股票列表
data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
###############################
#query_symbol
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
symbol = list()

for row in results:
        symbol.append(str(row[1]))
####################################3
# deal new
for j in range(data.shape[0]):
        resu0 = list(data.iloc[data.shape[0] - 1 - j])
        resu = deal_stock_basic(data)
        if (resu[1]) not in symbol:
                print("新增")
                save_stock_basic(resu)
