import smtplib
import ssl
import requests
import argparse
from datetime import datetime, timedelta, timezone
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

def sendNotificationEmail(emailType, details, senderEmail, receiverEmail, emailPassword, url):
    yesterday = datetime.now() - timedelta(1)
    formatted_date = yesterday.strftime('%Y-%m-%d')
    current_date = datetime.now().strftime('%Y-%m-%d')

    if emailType == "error":
        server_url = details.get('serverUrl', 'Unknown Server')
        error_url = details.get('url', 'No URL provided')
        error_message = details.get('message', 'No error message provided')
        subject = f"Security audit Error Notification - {current_date} - Server: {server_url}"
        body = f"""
        <html>
            <body>
                <p>Hello,<br><br>
                   An error occurred while running the security audit script. Please visit to the below server to check details.<br><br>
                   <strong>Server URL:</strong> {server_url}<br>
                   <strong>Error Message:</strong> {error_message}<br><br>
                   Thanks.
               </p>
           </body>
           </html>
        """
    elif emailType == "newUser":
        subject = f"New Users Created on {formatted_date} - Server: {url}"
        table_rows = "".join([
            f"<tr><td>{user['firstName']}</td><td>{user['lastName']}</td><td>{user['emailAddress']}</td><td><a href='{details['url']}/ui-app/#/users/{user['id']}/detail/overview'>User Details</a></td></tr>"
            for user in details['users']
        ])
        body = f"""
        <html>
            <body>
                <p>Hello,<br><br>
                   The below users were created yesterday. Please click on the below links to check user details.<br><br>
                   <table border="1">
                       <tr><th>First Name</th><th>Last Name</th><th>Email Address</th><th>URL</th></tr>
                       {table_rows}
                   </table><br><br>
                   Thanks.
               </p>
           </body>
           </html>
        """
    sendEmail(subject, body, senderEmail, receiverEmail, emailPassword)

def checkUserCreationDate(users, url, senderEmail, receiverEmail, emailPassword):
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    new_users = []

    for user in users:
        if 'creationDate' in user and 'id' in user:
            createdDate = datetime.fromtimestamp(user['creationDate'] / 1000, timezone.utc).date()
            if createdDate == yesterday:
                new_users.append({
                    "firstName": user['firstName'],
                    "lastName": user['lastName'],
                    "emailAddress" : user['emailAddress'],
                    "id": user['id']
                })
            else:
                continue

    if new_users:
        details = {
            "users": new_users,
            "url": url
        }
        sendNotificationEmail("newUser", details, senderEmail, receiverEmail, emailPassword, url)

def getUsers(url, token, senderEmail, receiverEmail, emailPassword):
    headers = {'X-OS-API-TOKEN': token}
    start = 0
    maxResults = 100
    while True:
        try:
            response = requests.get(f"{url}rest/ng/users?start={start}&max={maxResults}", headers=headers)
            response.raise_for_status()
            users = response.json()
            if not users:
                break
            checkUserCreationDate(users, url, senderEmail, receiverEmail, emailPassword)
            start += 100
        except requests.exceptions.RequestException as e:
            sendNotificationEmail("error", {
                'serverUrl': url,  # Using URL from the config
                'url': url,
                'message': str(e)
            }, senderEmail, receiverEmail, emailPassword)
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
        sendNotificationEmail("error", {
            'serverUrl': url,
            'url': url,
            'message': str(e)
        }, senderEmail, receiverEmail, emailPassword, url)
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
        try:
            error_message = str(e)
            sendNotificationEmail("error", {
                'serverUrl': 'N/A',  # Assuming no specific URL for critical errors
                'url': 'N/A',
                'message': error_message
            }, config['senderEmail'], config['receiverEmail'], config['emailPassword'])
        except NameError:
            print(f"Critical error: {str(e)}. Config not loaded.")
