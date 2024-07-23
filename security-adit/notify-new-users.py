import requests
import argparse
import json
from datetime import datetime, timezone
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import ssl 

def sendEmail(firstName, lastName, url, senderEmail, receiverEmail, emailPassword):
    message = MIMEMultipart("alternative")
    message["Subject"] = "New User Created Today"
    message["From"] = senderEmail
    message["To"] = receiverEmail

    html = f"""
    <html>
        <body>
            <p>Hello,<br><br>
               The new user is created.<br><br>
               First Name: {firstName}<br>
               Last Name: {lastName}<br><br>
               Visit <a href="{url}">this link</a> for more details.<br><br>
               Thanks.
           </p>
       </body>
       </html>
    """
    part = MIMEText(html, "html")
    message.attach(part)

    context = ssl._create_unverified_context()

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls(context=context)
            server.login(senderEmail, emailPassword)
            server.sendmail(senderEmail, receiverEmail, message.as_string())
            print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

def checkUserCreationDate(users, url, token, senderEmail, receiverEmail, emailPassword):
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    for user in users:
        if 'creationDate' in user:
            createdDate = datetime.fromtimestamp(user['creationDate'] / 1000, timezone.utc).date()
            if createdDate == yesterday:
                sendEmail(user['firstName'], user['lastName'], url, senderEmail, receiverEmail, emailPassword)
                print(f"User created yesterday: {user['firstName']} {user['lastName']}")
        else:
            continue

def getUsers(url, token):
    headers = {'X-OS-API-TOKEN': token}
    start = 0
    max_results = 100
    while True:
        response = requests.get(f"{url}rest/ng/users?start={start}&max={max_results}", headers=headers)
        if response.status_code == 200:
            users = response.json()
            if not users:
                break
            checkUserCreationDate(users, url, token, config['senderEmail'], config['receiverEmail'], config['emailPassword'])
            start += 100
            max_results += 100
        else:
            print(f"Failed to retrieve users: {response.status_code}")
            print("Response:", response.text)
            break

def getToken(loginName, password, url):
    payload = {
        "loginName": loginName,
        "password": password
    }

    response = requests.post(url + 'rest/ng/sessions', json=payload)

    if response.status_code == 200:
        data = response.json()
        return data['token']
    else:
        print(f"Failed to retrieve token: {response.status_code}")
        print("Response:", response.text)
        return None

def readConfig(configFile):
    config = {}
    with open(configFile, 'r') as file:
        for line in file:
            name, value = line.strip().split('=', 1)
            config[name.strip()] = value.strip()
    return config

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Notify new users and get token')
    parser.add_argument('configFile', type=str, help='Path to the configuration file')
    args = parser.parse_args()
    config = readConfig(args.configFile)
    token = getToken(config['loginName'], config['password'], config['url'])
    if token:
        getUsers(config['url'], token)
