import csv
import mysql.connector as msql
from mysql.connector import Error
import logging
import csv
import datetime  
from datetime import date, timedelta
import logging
import traceback
import sys

print(f"About to Start inserting into terminal_parameters:\n \n \n \n \n \n ")
path='C:/python_work/registration/details/BATCH.csv'
try:
    with open(path, newline='') as f:
        reader = csv.reader(f)
        my_list = list(reader)
       
except:
    print(f"The file does not exist in the location")
    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
    logging.warning(f'The file does not exist in the location ') 

else:

    list_items=[]
    parameter_count=0
    for item in my_list:
        with open(path, newline='') as f:
            reader = csv.reader(f)
            my_list = list(reader)
            contents1=my_list
            list_items.append(contents1)
            textfile = open("C:/python_work/registration/sql.txt", "a")

            
            ng='NG'
            position1=str(item[1]+"                    ")
            pos=position1[:23]
            print(position1)
            position2=str(item[17]+"           ")
            pos1=position2[:13]
            sql=f"INSERT INTO `terminal_parameters` (`terminal_id`,`merchant_id`,`transaction_timeout`,`currency_code`," \
                 f"`country_code`,`call_home_time`,`merchant_category_code`,`merchant_location_name_address`)" \
                 f"VALUES ('{item[9]}','{item[0]}',45,'566','566',1,'{item[16]}','{pos}{pos1}{item[17]}{ng}');"
           
            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
            logging.warning(f'Total of {parameter_count} was registered ') 

            print(sql)
            textfile.write(sql + "\n")
            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
            logging.warning(f'SQl statement are now being written to file named sql {item[9]}:{sql}. ') 
            try:
                conn = msql.connect(host='localhost', database='housing_data', user='root', password='wewedewew')
                if conn.is_connected():
                    cursor = conn.cursor()
                    cursor.execute("select database();")
                    record = cursor.fetchone()
                    print("You're connected to database: ", record)
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'You are connected to database and have inserted {item[9]}:{sql}')
                    cursor.execute(sql)
                    print("Record inserted")
                    # the connection is not auto committed by default, so we must commit to save our changes
                    conn.commit()
                    print("Data insertion has ended")
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Data insertion has ended')
            except Error as e:
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Duplicated or database connection error ')
            parameter_count=parameter_count+ 1
            print(f"A total of {parameter_count} terminal parameters were registered")
            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
            logging.warning(f'A total of {parameter_count} terminal parameters were registered')
finally:

    



    print(f"About to Start inserting into transaction_keys:\n \n \n \n \n \n ")

list_items=[]
keys_count=0
for item in my_list:
    with open(path, newline='') as f:
        reader = csv.reader(f)
        my_list = list(reader)
        contents1=my_list
        list_items.append(contents1)
        textfile = open("C:/python_work/registration/sql.txt", "a")


        sql2=f"INSERT INTO `transaction_keys` (`local_port`,`terminal_id`,`master_key`,`working_key`,`master_key_check_value`," \
             f"`working_key_check_value`,`master_key_change_count`,`working_key_change_count`,`master_key_last_date_change`," \
             f"`working_key_last_date_change`,`terminal_pin_key`,`terminal_pin_key_check_value`,`terminal_pin_key_change_count`," \
             f"`terminal_pin_key_last_date_change`,`session_master_key`,`session_master_check_value`,`session_master_key_2`," \
             f"`bdk_pin_block`,`bdk_pin_block_check_value`,`bdk_pin_block_change_count`,`bdk_pin_block_last_date_change`," \
             f"`bdk_track2`,`bdk_track2_check_value`,`bdk_track2_change_count`,`bdk_track2_last_date_change`,`bdk_emv`," \
             f"`bdk_emv_check_value`,`bdk_emv_change_count`,`bdk_emv_last_date_change`,`master_or_bdk`)" \
            f"VALUES ({item[26]},'{item[9]}','EDWD455DFDGDGDGDR5686969696969696','DFER34376475858DDF6564644464G4','WE3433'," \
             f"   'ETTT5464',0,0,now(),now(),'45DDFGGJKKHFD45656577544','D345564FG',0," \
              f"  '2020-08-17 17:31:42','3ERSDGTDFSDDE466464646','2WEWEDDE','DGERT4565565DSFDFD777878'," \
              f"  '345676575757757CDGDGDHHFHFHHFHF','08D7B4',0,now(),'DFEERF44776876DFGGFHFGGFGG'," \
               f" '08D7B4',0,now(),'E344DDFETGHTIY56568666976699','ETEEYY4466',0,now(),0);"
        print(sql2)
        
        logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
        logging.warning(f'Total of {keys_count} was registered ') 
        textfile.write(sql2 + "\n")
        logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
        logging.warning(f'SQl statement are now being written to file named sql:{sql2}. ') 
        try:
            conn = msql.connect(host='localhost', database='housing_data', user='root', password='sfsfsff')
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                print("You're connected to database: ", record)
                logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'You are connected to database and have inserted:{sql2}')
                cursor.execute(sql2)
                print("Record inserted")
                # the connection is not auto committed by default, so we must commit to save our changes
                conn.commit()
                print("Data insertion has ended")
                logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'Data insertion has ended')
                

        except Error as e:
                logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'Error while connecting to MySQL ')
        keys_count+=1
        print(keys_count)
        logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
        logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
        logging.warning(f'Total of {keys_count} trwas registered ')




print(f"Doing Final Check..............................\n \n \n \n \n \n ")

path2=f'C:/python_work/registration/sql.txt'

try:
    with open(path2, 'r') as file_object:
        lines=file_object.read()
        print(f'file- sql.txt exist in C:/python_work/registration/ ')
        logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
        logging.warning(f'file- sql.txt exist in C:/python_work/registration/ ') 
    #print(lines)
except:
    
    #logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
    logging.warning(f'The file does not exist in the location: {path2}') 
    

else:
    contents1=lines.split('\n')
   #print(contents1)
    #del contents1[-1]
    #print(contents1)
    
    try:
        conn = msql.connect(host='localhost', database='housing_data', user='root', password='sdsffsff')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            logging.basicConfig(filename='log.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')
            logging.warning(f'You re connected to database: {record}') 
            print("Doing Final Check..............................", record)

        for line in contents1[:]:
                #print(line)  

                
            cursor.execute(line, tuple())
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
            logging.warning(f'Done')

        
