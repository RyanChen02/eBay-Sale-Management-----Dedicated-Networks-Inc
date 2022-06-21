import email
from unittest.mock import MagicMock
import pyodbc
import pandas as pd
import datetime as dt
import datetime as dt
import os
from collections import defaultdict
from requests_oauthlib import OAuth2Session
from O365 import Account 
import schedule, time


##Building connection with database - "DEV-SQL01"
conn = pyodbc.connect(
DRIVER='{ODBC Driver 17 for SQL Server}',
SERVER='####',
DATABASE='####',
UID='####',
PWD='password',
trusted_connection='yes')


##Building connection with database - "DNI-SQL03"
cnx = pyodbc.connect(
DRIVER='{ODBC Driver 17 for SQL Server}',
SERVER='####',
DATABASE='####',
UID='pautomate',
PWD='####',
trusted_connection='no')


#import table2 
df_table2= pd.read_sql_query ('''SELECT inventory.itemnumber as 'Item Number', inventory.serialnumber as 'Serial Number',
                                 inventory.conditioncode as 'Condition Code',inventory.warehouse as 'Warehouse', st.cvcode as 'CV Code',
                                 stdetail.shipdate as 'Ship Date', st.stid as 'STID', inventory.inventoryid as 'Inventory ID'
                                 FROM st INNER JOIN inventory ON st.stid = inventory.stid INNER JOIN stdetail ON inventory.inventoryid = stdetail.selectedinventoryid
                                 WHERE (inventory.warehouse = 'EBAY' AND shipdate= cast(getdate() as Date))''', cnx)


#import table3 
df_table3= pd.read_sql_query ('''(SELECT itemnumber as 'Item Number', serialnumber as 'Serial Number', conditioncode as 'Condition Code',
                                 warehouse as 'Warehouse', cvcode as 'CV Code' , shipdate as 'Ship Date', stid as 'STID', inventoryid as 'Inventory ID',
                                 ebay_tracking as 'eBay Tracking' FROM ebay)''', conn)



#using table4 new dataframe table to compare table2 and table3
df_table4= pd.concat([df_table3,df_table2])
df_table4_drop= df_table4.drop_duplicates(subset=['Inventory ID'],keep=False,inplace=True) 
pd.DataFrame(df_table4_drop)



##O365 email API & send out email
def mail_update():
    reportname= 'Deals eBay Warehouse Sold'
    credentials= ('####',
                  '####'
                     )
    account= Account(credentials,
                     auth_flow_type='credentials',
                     tenant_id='####',
                      main_resource='####'
                      )
    account.authenticate()
    current_date= dt.date.today()
    email= account.new_message()


    if df_table4_drop is None : 
        body = str(current_date) + """  --- Deals eBay Warehouse Sold List """  + """</p>""" + """ Sorry, there is no new order yet."""
        print("There is no upcoming new order from eBay yet.")
        print("Email sent!")
    else: 
        body= str(current_date) + """  --- Deals eBay Warehouse Sold List """ + """</p>"""  + df_table4_drop.to_html() 
        print(df_table4_drop)
        print("Updated order sent.")
        print("Email sent!")
    
    email.sender.address ='####'
    email.to.add(['####'])
    email.subject= reportname + " ---- " + str(current_date)
    email.body= body
    email.send('####')



#Setting auto email sending system
schedule.every().monday.at("09:00").do(mail_update)
schedule.every().tuesday.at("09:00").do(mail_update)
schedule.every().wednesday.at("09:00").do(mail_update)
schedule.every().thursday.at("09:00").do(mail_update)
schedule.every().friday.at("09:00").do(mail_update)
schedule.every(30).minutes.do(mail_update)



##Closing database connection and print out result
conn.commit()
conn.close()
cnx.commit()
cnx.close()


while True: 
    schedule.run_pending()
    time.sleep(1)







