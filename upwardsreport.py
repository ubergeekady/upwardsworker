import boto3
import time
import json
import requests
import datetime
import rollbar
import re
import mysql.connector


def main():
    mysql_db = mysql.connector.connect(
        host="database-1.c7eqm4pzespm.us-east-1.rds.amazonaws.com",
        user="admin",
        passwd="n7!n*z*JZjADJ+85",
        database = "phoneparloan"
    )

    cursor = mysql_db.cursor(dictionary=True, buffered=True)
    query = "SELECT * FROM upwards_leads"
    cursor.execute(query)
    results = cursor.fetchall()

    for result in results:
        myList=[]
        myList.append(str(result['id']))
        myList.append(str(result['loan_id']))
        myList.append(str(result['customer_id']))

        data_dict = json.loads(result['data_dict'])
        myList.append(data_dict['first_name'])
        myList.append(data_dict['social_email_id'])
        myList.append(str(data_dict['mobile_number1']))
        myList.append(str(data_dict['salary']))
        myList.append(str(data_dict['applied_amount']))

        if result['document_submit_status'] == None:
            myList.append("false")
        else:
            myList.append("true")            


        with open("report.csv", "a") as myfile:
            myfile.write(",".join(myList)+"\n")



def debug(message):
    print("\033[92m DEBUG: {}\033[00m" .format(message))



if __name__ == '__main__':
    main()
