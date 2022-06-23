import email
import pyodbc
import pandas as pd
import datetime as dt
import os
from collections import defaultdict
from requests_oauthlib import OAuth2Session
from O365 import Account 
import schedule, time


##building connection with database - "DNI-SQL03"
cnx = pyodbc.connect(
    DRIVER='{ODBC Driver 17 for SQL Server}',
    SERVER='####',
    DATABASE='####',
    UID='pautomate',
    PWD='####',
    trusted_connection='no')


##building connection with database - "DEV-SQL01"
conn = pyodbc.connect(
    DRIVER='{ODBC Driver 17 for SQL Server}',
    SERVER='####',
    DATABASE='####',
    UID='pautomate',
    PWD='####',
    trusted_connection='no')

cursor= conn.cursor()


##table 1 & 2 --- pull out variables from st and inventory tables as First and Second Table
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


df_table2 =  pd.read_sql_query ("""SELECT itemnumber AS 'item number'
	,serialnumber AS 'serial number'
	,conditioncode AS 'condition code'
	,warehouse AS 'warehouse'
	,cvcode AS 'cv code'
	,shipdate AS 'ship date'
	,stid AS 'stid'
	,inventoryid AS 'inventory id'
FROM ebay 
WHERE shipdate = cast(getdate() AS DATE)
""", conn)



##using merge function to see two tables' differences
merge= df_table1.merge(df_table2, how='left', left_on=['stid'], right_on=['stid'],suffixes=['_iq','_ebay'])
merge.fillna(0,inplace=True)



##storing eBay merge differences variables into a new table to "DEV-Ryan" database
for i,e in merge.iterrows():
    if e['stid_ebay'] == 0:
        query= "INSERT INTO ebay values(?,?,?,?,?,?,?,?)"
        cursor.execute(query,e['Item Number'], e["Serial Number"],e["Condition Code"],e["Warehouse"],e["CV Code"],e["Ship Date"], e["STID"],e["Inventory ID"])
    else:
        continue



##checking tables' variables to send out proper email 
current_date= dt.date.today()
dataframe_html= merge.to_html()

if merge.empty == True: 
    pass   
else: 
    body=str(current_date) + """ --- Deals eBay Warehouse Sold List """  + dataframe_html 
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
    email= account.new_message()


##email address and sender info
    email.sender.address= '####'
    email.subject= reportname + " ---- " + str(current_date)
    email.body= body
    email.to.add (['####'])
    email.send ('####')
    print(merge)
    print("Updated order sent.")
    print("Email sent!")


##closing database connection and print out result
conn.commit()
conn.close()
cnx.commit()
cnx.close()

