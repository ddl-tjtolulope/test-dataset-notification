import os
import pandas as pd
import re
import json
import csv
import time
from pymongo import MongoClient
from collections import Counter
from bson import ObjectId
from pymongo import MongoClient
try:
    from urllib.parse import quote_plus
except ImportError:
    from urllib import quote_plus
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

MONGO_USERNAME="xxxxx"
MONGO_PASSWORD="xxxxx"
# MONGO_USERNAME="admin"
# MONGO_PASSWORD="BICK0NoMy09wowSgZsxDmz8t4EYeoOFT"

# Email variables:
FROM_EMAIL = "tj.tolulope@dominodatalab.com"
client = MongoClient(
    'mongodb://mongodb-replicaset.domino-platform.svc.cluster.local:27017',
    username = MONGO_USERNAME,
    password = MONGO_PASSWORD,
    authSource = 'domino',
    authMechanism = 'SCRAM-SHA-1'
)
db = client['domino']

def get_all(collection_name, filter={}, projection=None):
    return list(client["domino"][collection_name].find(filter, projection))

def export_project_datasets(writer,projects):
    snapshots = {str(row['_id']): row for row in get_all('datasetrw_snapshot')}
    print("Exporting Project Datasets.")
    tic = time.time()
    N = 0
    for row in get_all('datasetrw'):
        N += 1
        try:
            dataset_id = str(row['_id'])
            main_snapshot_id = row.get('readWriteSnapshotId')
            snapshot_ids = set(row.get('snapshots') + [main_snapshot_id])
            project_id = str(row['metadata']['labels']['project-object-id'])

            for snapshot_id in snapshot_ids:
                primary = (main_snapshot_id == snapshot_id)
                snapshot = snapshots[str(snapshot_id)]
#From project ID find the user ID from project collection
#From user Id get the user email
#then call the function in Datasets.py passing that email.
                

                output = {
                    "author_id": snapshot.get('author', snapshot.get('metadata', {}).get('authors', [None])[0]),
                    "firstName": (lambda user_owner_id:
                                    client["domino"].users.find_one(
                                        {"_id": ObjectId(user_owner_id)},
                                        {"_id": 0, "firstName": 1}
                                    ).get("firstName") if user_owner_id else None
                                 )(snapshot.get('author', snapshot.get('metadata', {}).get('authors', [None])[0])),
                    "email": (lambda user_owner_id:
                                    client["domino"].users.find_one(
                                        {"_id": ObjectId(user_owner_id)},
                                        {"_id": 0, "email": 1}
                                    ).get("email") if user_owner_id else None
                                 )(snapshot.get('author', snapshot.get('metadata', {}).get('authors', [None])[0])),
                    "loginId": (lambda loginId: 
                                    client["domino"].users.find_one(
                                        {"_id": ObjectId(loginId)},
                                        {"_id": 0, "loginId": 1}
                                    ).get("loginId", {}).get("id") if loginId else None
                                )(snapshot.get('author', snapshot.get('metadata', {}).get('authors', [None])[0])),
                    "name": row.get("name"),
                    "project_id": str(project_id),
                    "project_name": projects[projects['id']==project_id].iloc[0]['name'],
                    "creation_date": pd.to_datetime(snapshot['metadata']['creationDateMillis'], unit='ms'),
                    "status": snapshot['status'].get('lifecycleStatus',None),
                    "id_snapshot": str(snapshot_id),
                    "id_dataset": dataset_id,
                    "is_partial_size": snapshot.get('stats', {}).get('isPartialSize'),
                    "claimName": snapshot.get('resource', {}).get('claimName'),
                    "dataset_path": row.get("datasetPath"),
                    "is_scratch": False,
                    "last_used_date": pd.to_datetime(snapshot.get('stats', {}).get('lastUsedDateMillis'), unit='ms'),
                    "last_user_id": snapshot.get('stats', {}).get('lastUser'),
                    "storage_size": snapshot.get('stats', {}).get('storageSize'),
                    "file_count": snapshot.get('stats', {}).get('fileCount'),
                    "version": snapshot.get('version'),
                    "is_primary": primary,
                    "path": snapshot.get('resourceId',None)
                }

                writer.writerow(output)
        except Exception as e:
            print(row)
            raise e
    elapsed = time.time() - tic
    print("Wrote {N} datasets in {elapsed:0.2f} seconds.".format(N=N,elapsed=elapsed))

def export_datasets(explorer_path):

    projects = pd.read_csv(f"{explorer_path}Projects.csv")
    f = open(f"{explorer_path}Datasets.csv", 'w')
    writer = csv.DictWriter(f, fieldnames=[
        'author_id',
        'firstName',
        'loginId',
        'email',
        'creation_date', 
        'status', 
        'id_snapshot', 
        'id_dataset', 
        'is_partial_size',
        'claimName',
        'dataset_path',
        'is_scratch',
        'is_primary', 
        'last_used_date', 
        'last_user_id', 
        'name', 
        'project_id', 
        'project_name',
        'storage_size', 
        'file_count',
        'version',
        'path'
    ])
    writer.writeheader()

    export_project_datasets(writer,projects)

    f.close()

def export_projects(explorer_path):
    print("Exporting Projects Table.")
    tic = time.time()
    N = 0
    with open(f"{explorer_path}Projects.csv", 'w') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "creation_date", 
            "id", 
            "is_archived", 
            "name", 
            "owner_id"
        ])
        writer.writeheader()
        for row in get_all('projects'):
            N += 1
            output = {
                "creation_date": pd.to_datetime(row['created']),
                "id": str(row['_id']),
                "is_archived": row['isArchived'],
                "name": row['name'],
                "owner_id": row['ownerId']
            }
            writer.writerow(output)
    elapsed = time.time() - tic
    print("Wrote {N} projects in {elapsed:0.2f} seconds.".format(N=N,elapsed=elapsed))

def export_users(explorer_path):
    print("Exporting Users Table.")
    tic = time.time()
    N = 0
    with open(f"{explorer_path}Users.csv", 'w') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "id", 
            "login_id", 
            "full_name"
        ])
        writer.writeheader()
        for row in get_all('users'):
            N += 1
            output = {
                "id": row['_id'],
                "login_id": row['loginId']['id'],
                "full_name": row['fullName']    
            }
            writer.writerow(output)
    elapsed = time.time() - tic
    print("Wrote {N} users in {elapsed:0.2f} seconds.".format(N=N,elapsed=elapsed))

if __name__=="__main__":
    explorer_path = os.environ.get("EXPLORER_OUTPUT_PATH","/mnt/data-explorer/")

    export_users(explorer_path)
    export_projects(explorer_path)
    export_datasets(explorer_path)

#create a data-explorer folder
    
#Read the Dataset csv file
#Extract the specific values from the fields with loginId, project_name, dataset_name, dataset_id based on the specific login ID
def extract_datasets_by_login_id(csv_file_path, target_login_id):
    try:
        # Read Datasets.csv
        #datasets = pd.read_csv("/mnt/data-explorer/Datasets.csv")
        # Filter datasets based on loginId
        #filtered_datasets = datasets[datasets['loginId'] == target_login_id]
        # Specify the path to your CSV file
        csv_file_path = '/mnt/data-explorer/Datasets.csv'
        # Open the CSV file
        with open(csv_file_path, newline='') as csvfile:
            # Create a CSV reader
            csv_reader = csv.DictReader(csvfile)
            # Skip the header row if it exists
            next(csv_reader, None)
            # Iterate through each row in the CSV file
            for row in csv_reader:
                # Access individual fields by index
                # Do something with the extracted fields (e.g., print them)
                #print(email)
                if int(row['storage_size']) > int(10000000):
                    storage_size=row['storage_size']
                    dataset_id=row['id_dataset']
                    email=row['email']
                    first_name=row['firstName']
                    name=row['name']
                    user_name=row['loginId']
                    project_name=row['project_name']
                    format_warning_email(name, user_name, project_name, dataset_id)
                    contents, subject = send_email(email, contents, subject)
                    
###New Code####
# Creates the email warning the user that the workspace is idle for more than 4 hours.
def format_warning_email(name, user_name, project_name, dataset_id):
    workspace_message = (
        'Dataset "{}" - https://tj-bms35193.cs.domino.tech/{}/{}/{}'.format(
            name, user_name, project_name, dataset_id
        )
    )

    email_text = """### Dataset Reminder ###
    
This is a reminder that you own the dataset which is currently consuming a lot of space. Please review the files accordingly to see if it is still needed.
$WORKSPACE_MESSAGE

Thank you,

Your Domino Admin Team"""

    email_html = """
    <html xmlns="http://www.w3.org/1999/xhtml">
    <head>
      <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>Inactive Stopped Workspace Reminder</title>
    </head>
    <body style="font-size: 16px; width: 100% !important;-webkit-text-size-adjust: 100%;-ms-text-size-adjust: 100%;margin: 0;padding: 0;background-color: #F2F4F7;font-family: &quot;Lato&quot;, &quot;Helvetica Neue&quot;, Helvetica, Arial, sans-serif;color: #494B4E; line-height: 1.6em;">
      <div style="padding-bottom: 30px; width: 100%; background-color: #FBFCFF;">
        <div class="statusbar" style="background-color: #4C89D6;min-height: 6px;width: 100%;margin-bottom: 30px;height: 6px;display: block"></div>
        <div class="container" style="max-width: 80%;margin: 30px auto;border: 1px solid #DFE2E5">
          <table cellpadding="0" cellspacing="0" border="0" id="backgroundTable" style="background-color: #FFF;margin: 0;padding: 0;width: 100% !important;line-height: 100% !important;border-collapse: collapse;mso-table-lspace: 0;mso-table-rspace: 0">
            <tr>
              <td style="text-align: left;border-collapse: collapse">
                <table cellpadding="0" cellspacing="0" border="0" align="center" style="width: 100%;margin: 15px 0;border-collapse: collapse;mso-table-lspace: 0;mso-table-rspace: 0">
                  <tr style="margin-bottom:30px;">
                    <td width="8%" valign="top" style="border-collapse: collapse"></td>
                    <td width="72%" valign="top" style="border-collapse: collapse;padding-top: 15px;padding-bottom:12px;">
                      <h3 style="margin:0; color: #4C89D6 !important;">Idle Running Workspace Reminder</h3>
                    </td>
                    <td width="12%" valign="top" style="text-align: right;border-collapse: collapse;padding-top: 12px;padding-bottom:12px;">
                    </td>
                    <td width="8%" valign="top" style="border-collapse: collapse"></td>
                  </tr>
                </table>
                <table cellpadding="0" cellspacing="0" border="0" align="center" style="width: 100%;border-collapse: collapse;mso-table-lspace: 0;mso-table-rspace: 0">
                  <tr style="margin-bottom:30px;">
                    <td width="8%" valign="top" style="border-collapse: collapse"></td>
                    <td width="84%" valign="top" style="border-collapse: collapse">
                      <div style="background: #DFE2E5; height: 1px; width: 100%;"></div>
                    </td>
                    <td width="8%" valign="top" style="border-collapse: collapse"></td>
                  </tr>
                </table>
                <table cellpadding="0" cellspacing="0" border="0" align="center" style="width: 100%;margin: 20px 0 40px 0;border-collapse: collapse;mso-table-lspace: 0;mso-table-rspace: 0">
                  <tr>
                    <td width="8%" valign="top" style="border-collapse: collapse"></td>
                    <td width="84%" valign="top" style="border-collapse: collapse;">
                      <p style="line-height: 1.6em;font-size: 16px;margin-bottom: 40px;font-weight: 400;color: #494B4E;">This is a reminder that you own the following workspace which has been running idly for longer than 4 hours. Running idle workspaces accumulate storage and compute costs. Please take a moment and stop any workspaces that you are not actively using. The following workspace will be automatically stopped if still running idly for 24+ hours:
                        <br />
                        <br />$WORKSPACE_MESSAGE</p>
                    </td>
                    <td width="8%" valign="top" style="border-collapse: collapse"></td>
                  </tr>                
                  <tr>
                    <td width="8%" valign="top" style="border-collapse: collapse"></td>
                    <td width="84%" valign="top" style="border-collapse: collapse">
                      <p><br /><br />Thank you,<br /> <br /><br />Your Domino Admin Team</p>
                    </td>
                    <td width="8%" valign="top" style="border-collapse: collapse"></td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
        </div>
        <div style="max-width: 80%; margin: 30px auto;">
          <table cellpadding="0" cellspacing="0" border="0" align="center" style="margin: 15px 0;border-collapse: collapse;mso-table-lspace: 0;mso-table-rspace: 0;width: 100%">
            <tr style="margin-bottom:30px;">
              <td width="2%" valign="top" style="border-collapse: collapse"></td>
                <td width="48%" valign="top" style="text-align: right;font-size: 13px;border-collapse: collapse">
                  You are receiving this email because your Domino admin has configured these notifications.<br />Please contact them if you have any questions.
                </td>
                <td width="2%" valign="top" style="border-collapse: collapse"></td>
            </tr>
          </table>
        </div>
      </div>
    </body>
    </html>
    """
    email_html = email_html.replace("$WORKSPACE_MESSAGE", "".join(workspace_message))

    email_text = email_text.replace("$WORKSPACE_MESSAGE", "".join(workspace_message))

    return dict(html=email_html, text=email_text)

#chane to to email 
# Sends an email to the to address with the contents built from the format functions.
def send_email(email, contents, subject):
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = FROM_EMAIL
    msg["To"] = email
    # msg['Cc'] = ""

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(contents["text"], "plain")
    part2 = MIMEText(contents["html"], "html")

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)

    # Send the message via local SMTP server.
    s = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    s.ehlo()
    s.starttls()
    s.ehlo()
    # s.login(SMTP_USERNAME, SMTP_PASSWORD)
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    # s.sendmail(msg['From'],  msg["To"].split(",") + msg["Cc"].split(","), msg.as_string())
    s.sendmail(msg["From"], msg["To"], msg.as_string())
    s.quit()
    return contents, subject
