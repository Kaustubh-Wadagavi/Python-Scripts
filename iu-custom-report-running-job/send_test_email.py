import smtplib
from email.mime.text import MIMEText
from datetime import datetime

def send_email(subject: str, body: str, sender: str, password: str, recipient: str, smtp_server: str, smtp_port: int = 587):
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender, password)
            server.send_message(msg)
            print(f"[{datetime.now()}] Email sent successfully.")

    except Exception as e:
        print(f"[{datetime.now()}] Failed to send email: {e}")

if __name__ == "__main__":
    # ✅ Replace these with your actual credentials and target
    sender_email = "your_email@example.com"
    sender_password = "your_app_password_or_real_password"
    recipient_email = "recipient@example.com"
    smtp_server = "smtp.gmail.com"  # or another SMTP server like smtp.office365.com

    subject = "Test Email"
    body = "This is a test email sent at " + str(datetime.now())

    send_email(subject, body, sender_email, sender_password, recipient_email, smtp_server)
