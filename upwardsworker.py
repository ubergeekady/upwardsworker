import boto3
import time
import json
import requests
import datetime
import re
import mysql.connector

appconfig={}

def main():
    sqs_client = boto3.client(
        'sqs',
        aws_access_key_id=appconfig["aws_access_key_id"],
        aws_secret_access_key=appconfig["aws_secret_access_key"],
        region_name='us-east-2'
    )

    mysql_db_release = mysql.connector.connect(
        host=appconfig["mysql_host"],
        user=appconfig["mysql_user"],
        passwd=appconfig["mysql_passwd"],
        database = appconfig["mysql_db"]
    )

    mysql_db_uat = mysql.connector.connect(
        host=appconfig["uat_mysql_host"],
        user=appconfig["uat_mysql_user"],
        passwd=appconfig["uat_mysql_passwd"],
        database = appconfig["uat_mysql_db"]
    )

    debug("Starting Upwards Worker....")

    while True:
        debug("Checking SQS (Wait time 20 seconds)....")
        response = sqs_client.receive_message(
            QueueUrl=appconfig["sqs_queue_url"],
            AttributeNames=['All'],
            MessageAttributeNames=['string'],
            WaitTimeSeconds=20
        )
        try:
            messages = response['Messages']
        except KeyError:
            debug('No messages in the queue!')
            continue

        debug("Got Message")
        message = messages[0]
        debug(message)
        ReceiptHandle = message["ReceiptHandle"]

        try:
            messageBody = json.loads(message["Body"])

            with open("logs.txt", "a") as myfile:
                myfile.write(message["Body"]+"\n")

            debug(messageBody)
            mode = messageBody["mode"]
            messageType = messageBody["messageType"]
            lead_tracker_id = messageBody["lead_tracker_id"]
            user_id = messageBody["user_id"]
            
        except:
            debug("Removing message from queue due to exception")
            sqs_client.delete_message(QueueUrl=appconfig["sqs_queue_url"],ReceiptHandle=ReceiptHandle)
            continue

        if messageType=="createLead":
            if appconfig["UAT"]:
                debug("Uat DB")
                mysql_db = mysql_db_uat
            else:
                debug("Prod DB")
                mysql_db = mysql_db_release
                
            debug("createLead API")
            debug(appconfig["UAT"])
            debug(mysql_db)
            cursor = mysql_db.cursor(dictionary=True, buffered=True)     
            debug(lead_tracker_id)
            query = "SELECT * FROM leads WHERE lead_tracker_id = '"+lead_tracker_id+"';"
            cursor.execute(query)
            result = cursor.fetchone()
            debug(result)
            if result is None:
                debug("Removing message from queue user already exist in their platform")
                sqs_client.delete_message(QueueUrl=appconfig["sqs_queue_url"],ReceiptHandle=ReceiptHandle)
                continue    

            data_dict={}
            full_name = result['name'].split(" ", 1)
            first_name = full_name[0]
            try:
                last_name = full_name[1]
            except:
                last_name = full_name[0]
            data_dict["first_name"]=first_name
            data_dict["last_name"]=last_name
            data_dict["pan"]=result['pan_card']
            data_dict["affiliate_loan_identifier"] = lead_tracker_id

            if result['gender']=='MALE':
                data_dict["gender"]='Male'
            elif result['gender']=='FEMALE':
                data_dict["gender"]='Female'
            else:
                data_dict["gender"]='Male'

            try:
                d = datetime.datetime.strptime(result['dob'], '%d/%m/%Y').date()
                data_dict["dob"]=d.strftime('%Y-%m-%d')
            except:
                data_dict["dob"]="1986-12-26"
            

            data_dict["social_email_id"]=result['emailid']
            data_dict["mobile_number1"]=result['mobile']

            if result['stay_type']=='rented':
                data_dict["current_residence_type_id"]=3
            elif result['stay_type']=='owned':
                data_dict["current_residence_type_id"]=4

            a = re.split(' |,',result['address'], maxsplit=1)
            address1 = a[0]
            try:
                address2 = a[1]
            except:
                address2 = a[0]
            if address2=='':
                address2 = address1
            if address1=='':
                address1 = address2
                
            data_dict["current_address_line1"]=address1
            if data_dict['current_address_line1'] == "" or data_dict['current_address_line1'] is None:
                data_dict["current_address_line1"] = "Not Available"
            data_dict["current_address_line2"]=address2
            if data_dict['current_address_line2'] == "" or data_dict['current_address_line2'] is None:
                data_dict["current_address_line2"] = "Not Available"
            data_dict["current_pincode"]=result['pincode']
            data_dict["current_city"]=result['state']
            data_dict["current_state"]=result['state']
            data_dict["current_residence_stay_category_id"]="2"
            data_dict["total_work_experience_category_id"]=1
            data_dict["current_employment_tenure_category_id"]=1
            
            if result['organization'] == "" or result['organization'] is None:
                data_dict["company"] = "Not Available"
            else:
                data_dict["company"]=result['organization']

            if result['type_of_employment']=='SALARIED':
                data_dict["employment_status_id"]=3
            elif result['type_of_employment']=='SELF EMPLOYED' or result['type_of_employment']=='SELF_EMPLOYED':
                data_dict["employment_status_id"]=2

            if result['modeofsalary']=='cash':
                data_dict["salary_payment_mode_id"]=1
            elif result['modeofsalary']=='bank' or result['modeofsalary']=='inBank':
                data_dict["salary_payment_mode_id"]=2
            elif result['modeofsalary']=='cheque':
                data_dict["salary_payment_mode_id"]=3
            else:
                data_dict["salary_payment_mode_id"]=4

            data_dict["work_email_id"]=result['emailid']
            data_dict["profession_type_id"]=21

            data_dict["salary"]=result['salary']
            
            if result['existing_emi']==None or result['existing_emi']==0:
                data_dict["existing_emi_amount"]=0
            else:
                data_dict["existing_emi_amount"]=result['existing_emi']

            data_dict["qualification_type_id"]=2
            data_dict["loan_purpose_id"]=7
            data_dict["applied_amount"]=result['loan_amt']

            
            headers={
                'Affiliated-User-Id': appconfig["Affiliated-User-Id"], 
                'Affiliated-User-Session-Token': appconfig["upwards_secret_key"],
                'Content-Type':'application/json'
            }
            r = requests.post(appconfig["upwards_newlead_api"], headers=headers, data=json.dumps(data_dict))

            try:
                response_text = json.loads(r.text)
                debug("Printing Response Message...")
                debug(response_text)
            except:
                debug("500 Exception removing message from queue")
                rollbar.report_exc_info()
                sqs_client.delete_message(QueueUrl=appconfig["sqs_queue_url"],ReceiptHandle=ReceiptHandle)
                continue

            customer_id = response_text["data"]["loan_data"]["customer_id"]
            loan_id = response_text["data"]["loan_data"]["loan_id"]
            debug("Updating upwards leads")
            debug("Customer Id:"+str(customer_id))
            debug("Loan id:"+str(loan_id))
            if loan_id:
                data_dict_escaped=json.dumps(data_dict).replace("'","")
                query = "INSERT INTO upwards_leads (`lead_tracker_id`, `data_dict`, `data_dict_response`, `loan_id`, `customer_id`) VALUES ('"+lead_tracker_id+"', '"+data_dict_escaped+"', '"+r.text+"', '"+str(loan_id)+"', '"+str(customer_id)+"');"
                cursor.execute(query)
                mysql_db.commit()
                query = "UPDATE leads SET aggregator_status='accepted' WHERE lead_tracker_id = '"+lead_tracker_id+"';"
                cursor.execute(query)
                mysql_db.commit()
            else:
                debug("User already exist removing from queue")
                query = "UPDATE leads SET aggregator_status='rejected' WHERE lead_tracker_id = '"+lead_tracker_id+"';"
                cursor.execute(query)
                mysql_db.commit()
                sqs_client.delete_message(QueueUrl=appconfig["sqs_queue_url"],ReceiptHandle=ReceiptHandle)
                continue


        if messageType=="completeKyc":
            if appconfig["UAT"]:
                mysql_db = mysql_db_uat
            else:
                mysql_db = mysql_db_release
                
                

            cursor = mysql_db.cursor(dictionary=True, buffered=True)
            query = "SELECT * FROM upwards_leads WHERE lead_tracker_id = '"+lead_tracker_id+"';"
            cursor.execute(query)
            result = cursor.fetchone()
            if result is None:
                debug("User not present in upwards_leads table, removing from sqs")
                sqs_client.delete_message(QueueUrl=appconfig["sqs_queue_url"],ReceiptHandle=ReceiptHandle)
                continue
            try:
                customer_id = result['customer_id']
                loan_id = result['loan_id']
                debug("Check the status of the user before hitting create lead\n")
                headers={
                'Affiliated-User-Id': appconfig["Affiliated-User-Id"], 
                'Affiliated-User-Session-Token': appconfig["upwards_secret_key"],
                'Content-Type':'application/json'
                }
                status_dict={}
                status_dict["customer_id" ] = customer_id
                status_dict["loan_id"] = loan_id
                status_dict["level_id"] = '1'
                r = requests.post(appconfig["upwards_loan_status"], headers=headers, data=json.dumps(status_dict))
                
                response_text = json.loads(r.text)
                debug(response_text)
                loan_status = response_text["data"]["loan_status"]
                debug("::::::LOAN STATUS::::::")
                debug(loan_status)
                if loan_status == "archieved":
                    debug("Old user removing message from sqs")
                    sqs_client.delete_message(QueueUrl=appconfig["sqs_queue_url"],ReceiptHandle=ReceiptHandle)
                
            except Exception as e:
                debug("Exception")
                debug(e)
                sqs_client.delete_message(QueueUrl=appconfig["sqs_queue_url"],ReceiptHandle=ReceiptHandle)
                continue

            query = "SELECT * FROM user_documents WHERE user_id="+str(user_id)+";"
            cursor.execute(query)
            results = cursor.fetchall()

            for result in results:
                debug("Printing Result/Response")
                debug(result)
                debug("\n")
                if result['document_name']=="PAN":
                    uploadDocument(loan_id,customer_id,3,result['extension'],result['pdf_password'],result['weblink'],appconfig["upwards_secret_key"])
                    uploadDocument(loan_id,customer_id,2,result['extension'],result['pdf_password'],result['weblink'],appconfig["upwards_secret_key"])
                if result['document_name']=="SELFIE":
                    uploadDocument(loan_id,customer_id,1,result['extension'],result['pdf_password'],result['weblink'],appconfig["upwards_secret_key"])
                if result['document_name']=="ADDRESS_PROOF":
                    uploadDocument(loan_id,customer_id,6,result['extension'],result['pdf_password'],result['weblink'],appconfig["upwards_secret_key"])
                if result['document_name']=="ADDRESS_PROOF_BACK":
                    uploadDocument(loan_id,customer_id,7,result['extension'],result['pdf_password'],result['weblink'],appconfig["upwards_secret_key"])
                if result['document_name']=="BANK STATEMENT":
                    uploadDocument(loan_id,customer_id,12,result['extension'],result['pdf_password'],result['weblink'],appconfig["upwards_secret_key"])
                if result['document_name']=="SALARY":
                    uploadDocument(loan_id,customer_id,9,result['extension'],result['pdf_password'],result['weblink'],appconfig["upwards_secret_key"])

            document_detail_dict={}
            document_detail_dict['loan_id']=loan_id
            document_detail_dict['customer_id']=customer_id
            headers={'Affiliated-User-Id': appconfig["Affiliated-User-Id"], 
                    'Affiliated-User-Session-Token': appconfig["upwards_secret_key"],
                    'Content-Type':'application/json'}                
            r = requests.post(appconfig["upwards_all_documents_status"],headers=headers, data=json.dumps(document_detail_dict))
            debug("--------STATUS--------")
            debug(r.headers)
            debug(r.status_code)
            debug(r.text)
            debug("--------STATUS END-------")
            query = "UPDATE upwards_leads SET all_document_status='"+r.text+"' WHERE lead_tracker_id = '"+lead_tracker_id+"';"
            cursor.execute(query)

            document_completed={}
            document_completed['loan_id']=loan_id
            document_completed['customer_id']=customer_id
            headers={'Affiliated-User-Id': appconfig["Affiliated-User-Id"], 
                    'Affiliated-User-Session-Token': appconfig["upwards_secret_key"],
                    'Content-Type':'application/json'}                
            r = requests.post(appconfig["upwards_document_submit"],headers=headers, data=json.dumps(document_completed))
            debug(r.headers)
            debug(r.status_code)
            debug(r.text)
            query = "UPDATE upwards_leads SET document_submit_status='"+r.text+"' WHERE lead_tracker_id = '"+lead_tracker_id+"';"
            cursor.execute(query)
            mysql_db.commit()

        debug("Removing message from sqs, finished processing")
        sqs_client.delete_message(QueueUrl=appconfig["sqs_queue_url"],ReceiptHandle=ReceiptHandle)




def uploadDocument(loan_id, customer_id,document_type_id, extension, password, document_link, upwards_secret_key):
    debug(str(document_type_id)+"::::::"+str(document_link))
    document_data_dict={}
    document_data_dict['loan_id']=loan_id
    document_data_dict['customer_id']=customer_id
    document_data_dict['document_type_id']=document_type_id
    document_data_dict['document_extension']='.'+extension

    if extension == "pdf":
        if password is not None:
            document_data_dict["password"] = password
    elif (document_data_dict['document_type_id'] == 12):
        document_data_dict['document_extension']=".pdf"
        if password is not None:
            document_data_dict["password"] = password

    headers={'Affiliated-User-Id': appconfig["Affiliated-User-Id"], 
            'Affiliated-User-Session-Token': appconfig["upwards_secret_key"],
            'Content-Type':'application/json'}
    debug(headers)
    debug(appconfig["upwards_document_upload_url"])
    r = requests.post(appconfig["upwards_document_upload_url"],headers=headers, data=json.dumps(document_data_dict))
    #req = r.prepare()
    #debug('{}\n{}\r\n{}\r\n\r\n{}'.format(
     #   '-----------START-----------',
      #  req.method + ' ' + req.url,
      #  '\r\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
      #  req.body,
    #))
    debug(json.dumps(document_data_dict))
    debug(r.headers)
    debug(r.status_code)
    debug(r.text)
    debug(document_link)
    imageResponse = requests.get(document_link, stream=True)
    response_text = json.loads(r.text)
    document_url = response_text["data"]["document"]
    debug("Calling PUT Method\n")
    debug(imageResponse)
    debug("response_text"+str(response_text))
    debug("doc_url"+str(document_url))
    headers = {'content-type': 'multipart/form-data'}
    r = requests.put(document_url, data=imageResponse.content, verify=True)
    debug(r.headers)
    debug(r.status_code)
    debug(r.text)
    document_status_dict={}
    document_status_dict['loan_id']=loan_id
    document_status_dict['customer_id']=customer_id
    document_status_dict['document_type_id']=document_type_id
    document_status_dict['status']='file_creation_success'
    headers={'Affiliated-User-Id': appconfig["Affiliated-User-Id"], 
            'Affiliated-User-Session-Token': appconfig["upwards_secret_key"],
            'Content-Type':'application/json'}                
    r = requests.post(appconfig["upwards_update_document_status"],headers=headers, data=json.dumps(document_status_dict))
    debug(r.headers)
    debug(r.status_code)
    debug(r.text)



def refresh_token():
    mysql_db_release = mysql.connector.connect(
        host=appconfig["mysql_host"],
        user=appconfig["mysql_user"],
        passwd=appconfig["mysql_passwd"],
        database = appconfig["mysql_db"]
    )

    mysql_db_uat = mysql.connector.connect(
        host=appconfig["uat_mysql_host"],
        user=appconfig["uat_mysql_user"],
        passwd=appconfig["uat_mysql_passwd"],
        database = appconfig["uat_mysql_db"]
    )
    mysql_db = mysql_db_release

    debug("Refreshing API Token....")
    debug("Auth URL"+appconfig["upwards_auth_api"])
    headers={
        'Content-Type':'application/json'
    }
    data_dict={}
    data_dict['affiliated_user_id']=int(appconfig["Affiliated-User-Id"])
    data_dict['affiliated_user_secret']=appconfig["affiliated_user_secret"]
    r = requests.post(appconfig["upwards_auth_api"], headers=headers, data=json.dumps(data_dict))

    response_text = json.loads(r.text)
    token = response_text["data"]["affiliated_user_session_token"]

    cursor = mysql_db.cursor(dictionary=True, buffered=True)
    query = "UPDATE aggregrators SET api_token='"+token+"' WHERE id = 6"
    cursor.execute(query)
    mysql_db.commit()

    appconfig["upwards_secret_key"] = token



def debug(message):
    if appconfig["debug"]:
        print("\033[92m DEBUG: {}\033[00m" .format(message))



if __name__ == '__main__':
    f = open("config.json")
    config = json.loads(f.read())
    appconfig["debug"] = config["debug"]
    appconfig["UAT"] = config["UAT"]
    appconfig["aws_access_key_id"] = config["aws_access_key_id"]
    appconfig["aws_secret_access_key"] = config["aws_secret_access_key"]
    appconfig["sqs_queue_url"] = config["sqs_queue_url"]

    appconfig["mysql_host"] = config["mysql_host"]
    appconfig["mysql_user"] = config["mysql_user"]
    appconfig["mysql_passwd"] = config["mysql_passwd"]
    appconfig["mysql_db"] = config["mysql_db"]

    appconfig["uat_mysql_host"] = config["uat_mysql_host"]
    appconfig["uat_mysql_user"] = config["uat_mysql_user"]
    appconfig["uat_mysql_passwd"] = config["uat_mysql_passwd"]
    appconfig["uat_mysql_db"] = config["uat_mysql_db"]
    
    
    if appconfig["UAT"]:
        appconfig["Affiliated-User-Id"] = '11'
        appconfig["affiliated_user_secret"] = "vkHUVD8siOfZfupIWzgdDaQ5HAE688Ny"
        appconfig["upwards_auth_api"] = "http://uat1.upwards.in/af/v1/authenticate/"
        appconfig["upwards_newlead_api"] = "http://uat1.upwards.in/af/v1/customer/loan/data/"
        appconfig["upwards_document_upload_url"] = "http://uat1.upwards.in/af/v1/customer/loan/document/"
        appconfig["upwards_update_document_status"] = "http://uat1.upwards.in/af/v1/customer/loan/document/status/"
        appconfig["upwards_all_documents_status"] = "http://uat1.upwards.in/af/v1/customer/loan/documents/detail/"
        appconfig["upwards_document_submit"] = "http://uat1.upwards.in/af/v1/customer/loan/document/submit/"
        appconfig["upwards_loan_status"] = "http://uat1.upwards.in/af/v1/customer/loan/status/"
    else:
        appconfig["Affiliated-User-Id"] = '9'
        appconfig["affiliated_user_secret"] = "vT5Kk77sC0q7rpFAjDA1oGgOvTTAl3yK"
        appconfig["upwards_auth_api"] = "http://leads.backend.upwards.in/af/v1/authenticate/"
        appconfig["upwards_newlead_api"] = "http://leads.backend.upwards.in/af/v1/customer/loan/data/"
        appconfig["upwards_document_upload_url"] = "http://leads.backend.upwards.in/af/v1/customer/loan/document/"
        appconfig["upwards_update_document_status"] = "http://leads.backend.upwards.in/af/v1/customer/loan/document/status/"
        appconfig["upwards_all_documents_status"] = "http://leads.backend.upwards.in/af/v1/customer/loan/documents/detail/"
        appconfig["upwards_document_submit"] = "http://leads.backend.upwards.in/af/v1/customer/loan/document/submit/"
        appconfig["upwards_loan_status"] = "http://leads.backend.upwards.in/af/v1/customer/loan/status/"

    while True:
        try:
            refresh_token()
            debug(appconfig["Affiliated-User-Id"])
            
            main()
        except KeyboardInterrupt:
            print("Interrupt received! Exiting cleanly...")
            raise
        except Exception as e:
            debug("Error")
            debug(e)   
            continue

