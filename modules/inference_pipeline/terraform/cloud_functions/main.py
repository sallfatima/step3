import base64
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import functions_framework
from cloudevents.http import CloudEvent

# TODO: This does not work yet


@functions_framework.cloud_event
def send_email(cloud_event: CloudEvent):
    try:
        message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode(
            "utf-8"
        )
        print(message_data)

        # Check if the message indicates a failure
        if '"newJobState":"FAILED"' in message_data:
            sender = os.getenv("EMAIL_SENDER")
            sender_password = os.getenv("EMAIL_PASSWORD")
            recipient = sender

            subject = "Batch Job Failure Notification"
            body = f"A Batch job has failed:\n\n{message_data}"

            # Create a MIMEMultipart message
            msg_html = MIMEMultipart("alternative")
            msg_html["From"] = sender
            msg_html["To"] = recipient
            msg_html["Subject"] = subject

            msg_body = MIMEText(body, "html")
            msg_html.attach(msg_body)
            msg_str = msg_html.as_string()

            # Send email (configure SMTP settings here)
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(sender, sender_password)
                server.sendmail(sender, recipient, msg_str)

    except Exception as e:
        print("Error occurred:", e)
