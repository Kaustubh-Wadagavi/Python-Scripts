import requests
import argparse
import json
from datetime import datetime, timezone, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import ssl

def sendEmail(subject, body, senderEmail, receiverEmail, emailPassword):
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = senderEmail
    message["To"] = receiverEmail

    part = MIMEText(body, "html")
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

def sendErrorEmail(error_message, senderEmail, receiverEmail, emailPassword):
    subject = "Script Error Notification"
    body = f"""
    <html>
        <body>
            <p>Hello,<br><br>
               An error occurred in the script.<br><br>
               Error Message: {error_message}<br><br>
               Thanks.
           </p>
       </body>
       </html>
    """
    sendEmail(subject, body, senderEmail, receiverEmail, emailPassword)

def sendNewUserEmail(firstName, lastName, url, senderEmail, receiverEmail, emailPassword):
    subject = "New User Created Today"
    body = f"""
    <html>
        <body>
            <p>Hello,<br><br>
               A new user was created.<br><br>
               First Name: {firstName}<br>
               Last Name: {lastName}<br><br>
               Visit <a href="{url}">this link</a> for more details.<br><br>
               Thanks.
           </p>
       </body>
       </html>
    """
    sendEmail(subject, body, senderEmail, receiverEmail, emailPassword)

def checkUserCreationDate(users, url, token, senderEmail, receiverEmail, emailPassword):
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    for user in users:
        if 'creationDate' in user:
            createdDate = datetime.fromtimestamp(user['creationDate'] / 1000, timezone.utc).date()
            if createdDate == yesterday:
                sendNewUserEmail(user['firstName'], user['lastName'], url, senderEmail, receiverEmail, emailPassword)
                print(f"User created yesterday: {user['firstName']} {user['lastName']}")
        else:
            continue

def getUsers(url, token, senderEmail, receiverEmail, emailPassword):
    headers = {'X-OS-API-TOKEN': token}
    start = 0
    max_results = 100
    while True:
        try:
            response = requests.get(f"{url}rest/ng/users?start={start}&max={max_results}", headers=headers)
            response.raise_for_status()
            users = response.json()
            if not users:
                break
            checkUserCreationDate(users, url, token, senderEmail, receiverEmail, emailPassword)
            start += 100
        except requests.exceptions.RequestException as e:
            error_message = f"Failed to retrieve users: {str(e)}"
            sendErrorEmail(error_message, senderEmail, receiverEmail, emailPassword)
            break

def getToken(loginName, password, url, senderEmail, receiverEmail, emailPassword):
    payload = {
        "loginName": loginName,
        "password": password
    }

    try:
        response = requests.post(url + 'rest/ng/sessions', json=payload)
        response.raise_for_status()
        data = response.json()
        return data['token']
    except requests.exceptions.RequestException as e:
        error_message = f"Failed to retrieve token: {str(e)}"
        sendErrorEmail(error_message, senderEmail, receiverEmail, emailPassword)
        return None

def readConfig(configFile):
    config = {}
    with open(configFile, 'r') as file:
        for line in file:
            name, value = line.strip().split('=', 1)
            config[name.strip()] = value.strip()
    return config

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Notify new users and get token')
        parser.add_argument('configFile', type=str, help='Path to the configuration file')
        args = parser.parse_args()
        config = readConfig(args.configFile)
        token = getToken(config['loginName'], config['password'], config['url'], config['senderEmail'], config['receiverEmail'], config['emailPassword'])
        if token:
            getUsers(config['url'], token, config['senderEmail'], config['receiverEmail'], config['emailPassword'])
    except Exception as e:
        error_message = str(e)
        sendErrorEmail(error_message, config['senderEmail'], config['receiverEmail'], config['emailPassword'])
