import email
import pyodbc
import pandas as pd
import datetime as dt
import os
from collections import defaultdict
from requests_oauthlib import OAuth2Session
from O365 import Account 
import schedule,time 

##Building connection with database - "DEV-SQL03-"
cnx = pyodbc.connect(
    DRIVER='{ODBC Driver 17 for SQL Server}',
    SERVER='####',',
    DATABASE='####',
    UID='pautomate',
    PWD='35XtkmV3irAnKn6yjzh0',
    trusted_connection='no')


##Table 1 --- pull out variables from st and inventory as First Table
df_table1= pd.read_sql_query ('''SELECT inventory.itemnumber as 'Item Number', inventory.serialnumber  as 'Serial Number', 
                              inventory.conditioncode as 'Condition Code',inventory.warehouse as 'Warehouse', st.cvcode as 'CV Code', 
                              stdetail.shipdate as 'Ship Date', st.stid as 'STID', inventory.inventoryid as 'Inventory ID' 
                              FROM st INNER JOIN inventory ON st.stid = inventory.stid INNER JOIN stdetail ON inventory.inventoryid = stdetail.selectedinventoryid 
                              WHERE (inventory.warehouse = 'EBAY' AND shipdate= cast(getdate() as Date))''', cnx)


##Table 2 --- pull out variables from st and inventory as Second Table
df_table2= pd.read_sql_query ('''SELECT inventory.itemnumber as 'Item Number', inventory.serialnumber  as 'Serial Number', 
                              inventory.conditioncode as 'Condition Code',inventory.warehouse as 'Warehouse', st.cvcode as 'CV Code', 
                              stdetail.shipdate as 'Ship Date', st.stid as 'STID', inventory.inventoryid as 'Inventory ID' 
                              FROM st INNER JOIN inventory ON st.stid = inventory.stid INNER JOIN stdetail ON inventory.inventoryid = stdetail.selectedinventoryid 
                              WHERE (inventory.warehouse = 'EBAY' AND shipdate= cast(getdate() as Date))''', cnx)


##Using Merge function to see two tables' differences
merge= df_table1.merge(df_table2, how='left', on=['Item Number','Serial Number','Condition Code','Warehouse',
                               'CV Code','Ship Date','STID','Inventory ID'])


##Building connection with database - "DEV-SQL01"
conn = pyodbc.connect(
    DRIVER='{ODBC Driver 17 for SQL Server}',
    SERVER='####',
    DATABASE='####',
    UID='####',
    PWD='password',
    trusted_connection='yes')
cursor= conn.cursor()


##Storing eBay merge differences into a new table
for i,e in merge.iterrows():
    query= "INSERT INTO ebay values(?,?,?,?,?,?,?,?)"
    cursor.execute(query,e['Item Number'], e["Serial Number"],e["Condition Code"],e["Warehouse"],e["CV Code"],e["Ship Date"], e["STID"],e["Inventory ID"])
    


##Converting ebay new orders to csv form
csv_data= merge.to_csv(r'C:\Users\ryan.chen\Desktop\EBAY_Notification_Module 1.cvs', index = [0], sep =(","))
reportname= 'Deals eBay Warehouse Sold'



##O365 email API & send out email
def mail():
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
    dataframe_html= merge.to_html()
    email= account.new_message()

    isempty= merge.empty 
    if isempty== True:
        body="""This is a daily report of Deals eBay Warehouse Sold:"""+"""</p>""" 
        + str(current_date) + """  --- Deals eBay Warehouse Sold List """ + dataframe_html + """</p>""" + """ Sorry, there is no new order yet."""
        print("There is no upcoming new order from eBay yet.")
        print("Email sent!")
    else: 
        body=str(current_date) + """ --- Deals eBay Warehouse Sold List """  + dataframe_html 
        print(merge)
        print("Updated order sent.")
        print("Email sent!")


    email.sender.address ='####'
    email.to.add(['####'])
    email.subject= reportname + " ---- " + str(current_date)
    email.body= body
    email.send('####')


#Setting auto email sending system
schedule.every().monday.at("13:31").do(mail)
schedule.every().tuesday.at("08:30").do(mail)
schedule.every().wednesday.at("08:30").do(mail)
schedule.every().thursday.at("08:30").do(mail)
schedule.every().friday.at("08:30").do(mail)


##Closing database connection and print out result
cursor.commit()
cursor.close()
cnx.close()


while True: 
    schedule.run_pending()
    time.sleep(1)



    
