import csv
import mysql.connector as msql
from mysql.connector import Error
import datetime  
from datetime import date, timedelta
import logging
import traceback
import sys

print(f"Doing Final Check..............................\n \n \n \n \n \n ")

path2=f'C:/python_work/registration/sql.txt'

try:
    with open(path2, 'r') as file_object:
        lines=file_object.read()
        print(lines)
        print(f'file- sql.txt exist in C:/python_work/registration/ ')
        logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
        logging.warning(f'file- sql.txt exist in C:/python_work/registration/ ') 
    #print(lines)
except:
    
    #logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
    logging.warning(f'The file does not exist in the location: {path2}') 
    

else:
    contents1=lines.split('\n')
    print(contents1)
    del contents1[-1]
    #print(contents1)
    
    try:
        conn = msql.connect(host='localhost', database='housing_data', user='root', password='Magfum12@')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            logging.basicConfig(filename='log.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')
            logging.warning(f'You re connected to database: {record}') 
            print("Doing Final Check..............................", record)

        for line in contents1[:]:
            print(line)  

                
            cursor.execute(line,tuple())
            #print("Record inserted/updated")
            #logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
            logging.warning(f'Doing Final Check..............................')
            print(f'Doing Final Check..............................')
                        # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()
            
        #logging.basicConfig(filename='log.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')
        logging.warning(f'Doing Final Check..............................')
        print("Doing Final Check..............................")  
        
    except Error as e:
            #logging.basicConfig(filename='log.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')
            logging.warning(f'Doing Final Check..............................')
            print('Done')
