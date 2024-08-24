import smtplib
import ssl
import requests
import argparse
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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

def sendNotificationEmail(subject, body, senderEmail, receiverEmail, emailPassword):
    sendEmail(subject, body, senderEmail, receiverEmail, emailPassword)

def getUsers(url, token):
    headers = {'X-OS-API-TOKEN': token}
    start = 0
    maxResults = 100
    all_users = []

    while True:
        try:
            response = requests.get(f"{url}rest/ng/users?start={start}&max={maxResults}", headers=headers)
            response.raise_for_status()
            users = response.json()
            if not users:
                break
            all_users.extend(users)
            start += 100
        except requests.exceptions.RequestException as e:
            sendNotificationEmail(
                subject=f"Error Fetching Users - {url} - {datetime.now().strftime('%Y-%m-%d')}",
                body=f"""
                <html>
                    <body>
                        <p>Hello,<br><br>
                           An error occurred while fetching the user list from the server.<br><br>
                           <strong>Server URL:</strong> {url}<br>
                           <strong>Error Message:</strong> {str(e)}<br><br>
                           Thanks.
                       </p>
                   </body>
                   </html>
                """,
                senderEmail=config['senderEmail'],
                receiverEmail=config['receiverEmail'],
                emailPassword=config['emailPassword']
            )
            return None

    return all_users

def formatDate(timestamp):
    try:
        return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return 'N/A'

def createUserListEmailBody(users, url):
    table_rows = "".join([
        f"<tr><td>{index + 1}</td><td>{user.get('firstName', 'N/A')}</td><td>{user.get('lastName', 'N/A')}</td><td>{user.get('emailAddress', 'N/A')}</td><td>{user.get('domain', 'N/A')}</td><td>{user.get('activityStatus', 'N/A')}</td><td>{formatDate(user.get('creationDate', 0))}</td><td><a href='{url}/ui-app/#/users/{user['id']}/detail/overview'>User Details</a></td></tr>"
        for index, user in enumerate(users)
    ])
    body = f"""
    <html>
        <body>
            <p>Hello,<br><br>
               Below is the list of all users present on the server.<br><br>
               <table border="1">
                   <tr><th>Sr.No.</th><th>First Name</th><th>Last Name</th><th>Email Address</th><th>Domain</th><th>Activity Status</th><th>Created On</th><th>URL</th></tr>
                   {table_rows}
               </table><br><br>
               Thanks.
           </p>
       </body>
       </html>
    """
    return body

def readConfig(configFile):
    config = {}
    with open(configFile, 'r') as file:
        for line in file:
            name, value = line.strip().split('=', 1)
            config[name.strip()] = value.strip()
    return config

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Send list of all users as email notification')
        parser.add_argument('configFile', type=str, help='Path to the configuration file')
        args = parser.parse_args()
        config = readConfig(args.configFile)

        # Get token
        payload = {
            "loginName": config['loginName'],
            "password": config['password']
        }
        try:
            response = requests.post(config['url'] + 'rest/ng/sessions', json=payload)
            response.raise_for_status()
            token = response.json().get('token')
        except requests.exceptions.RequestException as e:
            sendNotificationEmail(
                subject=f"Error Obtaining Token - {config['url']} - {datetime.now().strftime('%Y-%m-%d')}",
                body=f"""
                <html>
                    <body>
                        <p>Hello,<br><br>
                           An error occurred while obtaining the authentication token.<br><br>
                           <strong>Server URL:</strong> {config['url']}<br>
                           <strong>Error Message:</strong> {str(e)}<br><br>
                           Thanks.
                       </p>
                   </body>
                   </html>
                """,
                senderEmail=config['senderEmail'],
                receiverEmail=config['receiverEmail'],
                emailPassword=config['emailPassword']
            )
            exit(1)

        if token:
            users = getUsers(config['url'], token)
            if users is not None:
                body = createUserListEmailBody(users, config['url'])
                subject = f"List of All Users - {config['url']} - {datetime.now().strftime('%Y-%m-%d')}"
                sendNotificationEmail(subject, body, config['senderEmail'], config['receiverEmail'], config['emailPassword'])
            else:
                print("Failed to fetch user list.")
    except Exception as e:
        sendNotificationEmail(
            subject=f"Critical Error - {config['url']} - {datetime.now().strftime('%Y-%m-%d')}",
            body=f"""
            <html>
                <body>
                    <p>Hello,<br><br>
                       A critical error occurred while executing the script.<br><br>
                       <strong>Error Message:</strong> {str(e)}<br><br>
                       Thanks.
                   </p>
               </body>
               </html>
            """,
            senderEmail=config['senderEmail'],
            receiverEmail=config['receiverEmail'],
            emailPassword=config['emailPassword']
        )
