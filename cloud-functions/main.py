import base64
import json
import os
from datetime import datetime

import functions_framework
import pendulum
import requests
from cloudevents.http import CloudEvent
from google.cloud import firestore

webhook_url = os.environ.get(
    "WEBHOOK_URL", "Specified environment variable is not set."
)
red_code = "DC4731"
green_code = "50C878"


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    # Print out the data from Pub/Sub, to prove that it worked
    print(
        "Hello, " + base64.b64decode(cloud_event.data["message"]["data"]).decode() + "!"
    )
    pubsub_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
    pubsub_json = json.loads(pubsub_data)
    cost_amount = pubsub_json["costAmount"]
    budget_amount = pubsub_json["budgetAmount"]
    budget_display_name = pubsub_json["budgetDisplayName"]
    alertThresholdExceeded = pubsub_json.get("alertThresholdExceeded", None)
    project_id = get_alert_name(pubsub_json["budgetDisplayName"])

    if alertThresholdExceeded is None:
        if cost_amount <= budget_amount:
            print(f"No action necessary. (Current cost: {cost_amount})")
            return
    else:
        content = """
            Budget name '{}'<br>
            Current cost is <b>${}</b>.<br>
            Budget cost is <b>${}</b>.<br>
            💸 💸 💸 💸
        """.format(
            budget_display_name, cost_amount, budget_amount
        )
        title = "💸 Cost reaches {:.0f}% from {}".format(
            alertThresholdExceeded * 100, budget_display_name
        )
        print(f"############################################")
        print(f"budget threshold is {alertThresholdExceeded}")
        print(f"{title}")
        print(f"{content}")
        print(f"############################################")

        should_notify = check_to_notify_per_month(project_id)
        if should_notify:
            if alertThresholdExceeded == 0.75:
                send_teams(
                    webhook_url,
                    content=content,
                    title=title,
                    color=red_code,
                )
            elif alertThresholdExceeded == 1.0:
                ### do something differently from 75% ###
                send_teams(
                    webhook_url,
                    content=content,
                    title=title,
                    color=red_code,
                )
        else:
            return


def send_teams(
    webhook_url: str, content: str, title: str, color: str = "000000"
) -> int:
    """
    - Send a teams notification to the desired webhook_url
    - Returns the status code of the HTTP request
      - webhook_url : the url you got from the teams webhook configuration
      - content : your formatted notification content
      - title : the message that'll be displayed as title, and on phone notifications
      - color (optional) : hexadecimal code of the notification's top line color, default corresponds to black
    """
    response = requests.post(
        url=webhook_url,
        headers={"Content-Type": "application/json"},
        json={
            "themeColor": color,
            "summary": title,
            "sections": [{"activityTitle": title, "activitySubtitle": content}],
        },
    )
    return response.status_code  # Should be 200


def check_to_notify_per_month(project_id: str) -> bool:
    """_summary_
        check if already notify in Teams in that month or not
        by checking from Cloud Firestore
    Args:
        project_id (str): project id get from alert name

    Returns:
        bool: True when already notify and update data in Firestore, False to skip notify
    """
    db = firestore.Client()
    last_notify_ref = db.collection("budget-alert-notification-stats").document(
        project_id
    )
    current_timestamp = pendulum.now("Asia/Bangkok")

    try:
        last_notify_snapshot = last_notify_ref.get()
        if not last_notify_snapshot.exists:
            # Initialize with the current timestamp if it's the first time.
            last_notify_ref.set({"last_noti": current_timestamp})
            print("Never has notification before, should notify team")

            return True
        else:
            last_notify_in_bkk = convert_to_timestamp_with_bkk(
                last_notify_snapshot.get("last_noti")
            )

            print("last_notify_in_bkk: ", last_notify_in_bkk)
            print("current_timestamp: ", current_timestamp)
            # Check if a month has passed since the last notify.
            if has_one_month_passed(current_timestamp, last_notify_in_bkk):
                # Proceed with sending the notify to Microsoft Teams.

                # Update the last notify timestamp.
                last_notify_ref.update({"last_noti": current_timestamp})
                return True
            else:
                print("Skipping notify, All ready notify in this month.")
                return False
    except Exception as e:
        print("Error processing notify:", e)


def convert_to_timestamp_with_bkk(timestamp_from_firestore: datetime) -> datetime:
    last_notify_timestamp = timestamp_from_firestore.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    last_notify_timestamp = pendulum.parse(last_notify_timestamp, tz="UTC")
    last_notify_timestamp_in_bkk = last_notify_timestamp.in_tz("Asia/Bangkok")
    return last_notify_timestamp_in_bkk


def has_one_month_passed(
    current_timestamp: datetime, last_notify_timestamp: datetime
) -> bool:
    current_month, current_year = get_month_and_year(current_timestamp)
    last_notify_month, last_notify_year = get_month_and_year(last_notify_timestamp)
    return (current_year > last_notify_year) or (
        current_year == last_notify_year and current_month > last_notify_month
    )


def get_month_and_year(timestamp: datetime):
    month = timestamp.month
    year = timestamp.year
    return month, year


def get_alert_name(alert_name: str) -> str:
    """_summary_

    Args:
        alert_name (str): alert name from pubsub
        e.g. mils-project-bigquery-notification

    Raises:
        InvalidAlertNameException: raise alert name when there's no "-bigquery-notification" in alert name

    Returns:
        str: project name that cost is peak
    """
    if "-bigquery-notification" in alert_name:
        project_to_assign = alert_name.split("-bigquery-notification")[0]
        return project_to_assign
    else:
        raise InvalidAlertNameException(alert_name)


class InvalidAlertNameException(Exception):
    def __init__(self, alert_name, message="Alert name is not valid"):
        self.alert_name = alert_name
        self.message = message
        super().__init__(self.message)
