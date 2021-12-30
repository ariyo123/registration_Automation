import csv
import mysql.connector as msql
from mysql.connector import Error
import datetime  
from datetime import date, timedelta
import logging
import traceback
import sys
import openpyxl


try:
        conn = msql.connect(host='localhost', database='housing_data', user='root', password='Magfum12@')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            logging.basicConfig(filename='log.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')
            logging.warning(f'You re connected to database: {record}') 
            print("You're connected to database: ", record)

        wb = openpyxl.load_workbook('BATCH011_17_012_2021.xlsx')
        print(wb)
        sheet = wb['First Bank of Nigeria Plc'] 
        print(sheet)
        c = sheet['B10'].value 
        
       
        print(c)


        """for line in contents1[:]:
                #print(line)  

                
            cursor.execute(line, tuple())
            #print("Record inserted/updated")
            #logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
            logging.warning(f'Record inserted/updated:{line}')
            print(f'Record inserted/updated: {line}')
                        # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()"""
            
        
except Error as e:
     #logging.basicConfig(filename='log.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')
    logging.warning(f'Error while connecting to MySQL {CurrentDate}')
    print('Invalid connection credential')