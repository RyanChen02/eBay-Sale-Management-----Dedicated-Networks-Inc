import email
from unittest.mock import MagicMock
from numpy import empty
import pyodbc
import pandas as pd
import datetime as dt
import os
from collections import defaultdict
from requests_oauthlib import OAuth2Session
from O365 import Account 
import schedule, time


##building connection with database - "DEV-SQL01"
conn = pyodbc.connect(
DRIVER='{ODBC Driver 17 for SQL Server}',
SERVER='####',
DATABASE='####',
UID='####',
PWD='password',
trusted_connection='yes')

##building connection with database - "DNI-SQL03" 
cnx = pyodbc.connect(
    DRIVER='{ODBC Driver 17 for SQL Server}',
    SERVER='####',
    DATABASE='####',
    UID='pautomate',
    PWD='####',
    trusted_connection='no')


#import table2 from "DNI-SQL01" 
df_table2 =  pd.read_sql_query ("""SELECT itemnumber AS 'item number'
	,serialnumber AS 'serial number'
	,conditioncode AS 'condition code'
	,warehouse AS 'warehouse'
	,cvcode AS 'cv code'
	,shipdate AS 'ship date'
	,stid AS 'stid'
	,inventoryid AS 'inventory id'
    ,ebay_tracking AS 'ebay tracking'
FROM ebay 
WHERE shipdate = cast(getdate() AS DATE)
""", conn)


#import table3 from "DNI-SQL01" 
df_table1= pd.read_sql_query ('''SELECT item.itemnumber AS 'item number'
	,stdetail.serialnumber AS 'serial number'
	,stdetail.conditioncode AS 'condition code'
	,stdetail.warehouse AS 'warehouse'
	,st.cvcode AS 'cv code'
	,stdetail.shipdate AS 'ship date'
	,stdetail.stid AS 'stid'
	,stdetail.selectedinventoryid AS 'inventory id'
FROM stdetail
INNER JOIN item ON stdetail.itemid = item.itemid
INNER JOIN st  ON st.stid = stdetail.stid
WHERE (
		stdetail.warehouse = 'EBAY'
		AND shipdate = cast(getdate() AS DATE)
		)''', cnx)


#creating new table as table4 to compare variables in table2 and table3
df_table3= pd.concat([df_table1,df_table2])
df_table3_drop= df_table3.drop_duplicates(subset=['Inventory ID'],keep=False,inplace=True) 
df_table3_dataframe= pd.DataFrame(df_table3_drop)


##checking tables' variables to send out proper email 
current_date= dt.date.today()
dataframe_html= df_table3_dataframe.to_html()

if df_table3_dataframe.empty == True: 
    pass
else: 
    body= str(current_date) + """  --- Deals eBay Warehouse Sold List """ + """</p>"""  + df_table3_dataframe.to_html() 
    reportname= 'Deals eBay Warehouse Sold'


##O365 email API & send out email 
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
    email.sender.address ='####'
    email.to.add(['####'])
    email.subject= reportname + " ---- " + str(current_date)
    email.body= body
    email.send('####')
    print(df_table3_dataframe)
    print("Updated order sent.")
    print("Email sent!")

    
##closing database connection and print out result
conn.commit()
conn.close()
cnx.commit()
cnx.close()






