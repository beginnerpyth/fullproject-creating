import pandas as pd
import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel
import mysql.connector
bal=FastAPI()
data={1:'a',2:'b'}
class val(BaseModel):
    PRODUCT_ID:int
    PRODUCT_NAME:str
    CATEGORY:str
    PRICE:int
class val1(BaseModel):
    SALES_ID:int
    PRODUCT_ID:int
    QUANTITY:int
    SALES_PRICE:int
    CUSTOMER_REGION:str
class val2(BaseModel):
    PRODUCT_ID:int
    STOCK_QUANTITY:int
    REORDER_LEVEL:int






def mac():
    global conn#so i did global cause to make it globally not locally and it would send error cause everytike i send it locallly 
    #it doesnt work 
    conn=mysql.connector.connect(host='localhost',user='root',password='password@123',database='FULLPROJECT',unix_socket='/tmp/mysql.sock')
    return conn
#we saved into variable because if we dont then it creates multiple connection which is not good for cursor


@bal.get('/data/test/')
def recevie(sal:int):
    return data[sal]

@bal.get('/showproduct/')
def viewproduct(d:int):
    
    chec1=pd.read_sql(f'SELECT * FROM PRODUCT WHERE PRODUCT_ID = {d}',mac())
    
    trans=chec1.to_dict(orient='records')

    return trans
    
@bal.post('/PRODUCT/')
def product(sal:val):
    
    cur=mac().cursor()
    cur.execute('SELECT PRODUCT_ID FROM PRODUCT')
    b=cur.fetchall()
    check1=[x[0] for x in b]
    
    if sal.PRODUCT_ID in check1:
        return 'its already here'
    value=[(sal. PRODUCT_ID,
    sal.PRODUCT_NAME,
    sal.CATEGORY,
    sal.PRICE)]
    
    cur.executemany('INSERT INTO PRODUCT VALUES(%s,%s,%s,%s,NOW())',value)
    mac().commit()##i onlly need when conn.commit when inserting uploading delete
    cur.close()
@bal.get('/SHOW_SALES/')
def showsales(c:int):
  
  show_sales=pd.read_sql(f'SELECT * FROM SALES WHERE PRODUCT_ID = {c}',mac())
  
  sales=show_sales.to_dict(orient='records')#it converts the sql into dictionary form
  return sales
 
@bal.post('/SALES/')
def sales(chal:val1):
    two=mac()
    cur=two.cursor()
    value1=[(chal.SALES_ID,
    chal.PRODUCT_ID,
    chal.QUANTITY,
    chal.SALES_PRICE,
    chal.CUSTOMER_REGION)]
    cur.executemany('INSERT INTO SALES(SALES_ID,PRODUCT_ID,QUANTITY,SALES_PRICE,CUSTOMER_REGION,SALES_DATE)VALUES(%s,%s,%s,%s,%s,NOW())',value1)
    two.commit()
    cur.close()


@bal.get('/SHOW_INVENTORY')
def show_inventory(b:int):
 
 show_inventory=pd.read_sql(f'SELECT * FROM INVENTORY WHERE PRODUCT_ID = {b}',mac())
 grand=show_inventory.to_dict(orient='records')
 
 return grand

@bal.post('/INVENTORY/')
def inventory(mal:val2,):
    one=mac()#we stored in one cause not to make any other duplicate connections
    cur=one.cursor()
    cur.execute('SELECT PRODUCT_ID FROM INVENTORY WHERE PRODUCT_ID')

    a = cur.fetchall()
    
    loop=[x[0] for x in a]

    if  mal.PRODUCT_ID in loop:
        return 'its already here'
    value2=(mal.PRODUCT_ID,
    mal.STOCK_QUANTITY,
    mal.REORDER_LEVEL)
    
    
    cur.execute('INSERT INTO INVENTORY VALUES(%s,%s,%s,NOW())',value2)
   
    one.commit()#it saves the data
    return 'finally inserted'
@bal.post('/SALES_SUMMARY/')
def sales_summary():
    three=mac()
    cur=three.cursor()
    sales_sum=pd.read_sql('SELECT * FROM SALES',three)
    sales_sum['TOTAL_REVENUE']=sales_sum['SALES_PRICE']*sales_sum['QUANTITY']
    sales_sum['TOTAL_QUANTITY']=sales_sum.groupby('PRODUCT_ID').agg({'QUANTITY':'sum'})
    
    tq=sales_sum.groupby('PRODUCT_ID',as_index=False).agg({'TOTAL_REVENUE':'sum','TOTAL_QUANTITY':'sum'})
    sales_sum1=tq.to_records(index=False)
    sales_sum1_list=sales_sum1.tolist()
    sql = "INSERT INTO SALES_SUMMARY (PRODUCT_ID, TOTAL_REVENUE, TOTAL_QUANTITY, LAST_UPDATED) VALUES (%s, %s, %s, NOW())"
    cur.executemany(sql, sales_sum1_list)
    three.commit()
@bal.get('/sales_summary/')
def sales_sum(a:int):
    summ=mac()
    sales_sum_show=pd.read_sql(f'SELECT * FROM SALES_SUMMARY WHERE PRODUCT_ID = {a}',summ)
    sales_sum_show1=sales_sum_show.to_dict(orient='records')
    return sales_sum_show1
@bal.post('/low_stock/')
def low_stock():
    low=mac()
    cur=low.cursor()
    inven=pd.read_sql('SELECT * FROM INVENTORY',low)
    for_stock=inven.groupby('PRODUCT_ID',as_index=False).agg({'STOCK_QUANTITY':'sum','REORDER_LEVEL':'sum'})
    for_stock_records=for_stock.to_records(index=False)
    for_stock_list=for_stock_records.tolist()
    cur.execute('DELETE FROM LOW_STOCK')
    cur.executemany('INSERT INTO LOW_STOCK(PRODUCT_ID,STOCK_QUANTITY,REORDER_LEVEL)VALUES(%s,%s,%s)',for_stock_list)
    low.commit()


    print('commit succesfully')


    

@bal.get('/low_stock_show/')
def low_stock_show(c:int):
    low1=mac()
    low_stock_show1=pd.read_sql(f'SELECT * FROM LOW_STOCK WHERE PRODUCT_ID = {c}',low1)
    low_stock_records=low_stock_show1.to_dict(orient='records')
    return low_stock_records





    
    


    
    
    






