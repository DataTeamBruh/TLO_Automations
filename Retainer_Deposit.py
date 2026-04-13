import os
import datetime
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth


# CONFIG
TEST_MODE = True

load_dotenv()

retainer_url = os.getenv("retainer_url")
username = os.getenv("username")
password = os.getenv("password")
channel_id = os.getenv("channel_id")
#channel_id = "C0A97PKJ9JL"
slack_token = os.getenv("slack_token")

# FETCH RETAINER DATA

def get_odata_dataframe(url, username, password):
    try:
        response = requests.get(
            url,
            auth=HTTPBasicAuth(username, password),
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        if "value" in data:
            df = pd.DataFrame(data["value"])
            print("✅ Data successfully loaded into DataFrame")
            return df

        raise ValueError("No 'value' key found in JSON response")

    except Exception as e:
        print(f"❌ Error loading OData: {e}")
        return pd.DataFrame()


# SLACK SENDER

def send_message_as_user(channel_id, message, user_token, test_mode=True):

    if test_mode:
        print("\nTEST MODE — message NOT sent to Slack\n")
        print(message)
        print("-" * 80)
        return

    url = "https://slack.com/api/chat.postMessage"

    headers = {
        "Authorization": f"Bearer {user_token}",
        "Content-type": "application/json"
    }

    payload = {
        "channel": channel_id,
        "text": message
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200 and response.json().get("ok"):
        print("✅ Message sent successfully")
    else:
        print("❌ Failed to send message:", response.text)


# BUILD MESSAGE

def build_message(df, lower_bound, upper_bound, title):

    today = datetime.date.today()

    df_check = df[
        (df["UTILIZATION"] >= lower_bound) &
        (df["UTILIZATION"] < upper_bound)
    ]

    message = (
        f"{title}"
        "\nPlease keep an eye on them to help prevent any overspending."
        "\nStaying on top of these ensures everything runs smoothly.\n\n"
        f"As at {today.strftime('%A, %d %B %Y')}:\n"
    )

    if df_check.empty:
        message += "\n_No retainers matched this threshold._"
        return message

    lines = []

    for _, row in df_check.iterrows():

        name = row.get("ENTITY_CLIENT_NAME", row.get("CLIENT", "Unknown client"))
        retainer_name = row.get("NAME", "Unknown retainer")

        util = row["UTILIZATION"]
        used = row["USEDVALUE"]
        budget = row["INVOICEDVALUE"]

        lines.append(
            f"• {name}, {retainer_name}: *{util:.1f}%* used "
            f"(R{used:,.0f} of R{budget:,.0f})"
        )

    message += "\n".join(lines)

    return message

# MAIN JOB

def run_retainer_job():

    df_retainer = get_odata_dataframe(retainer_url, username, password)

    if df_retainer.empty:
        print("⚠️ No retainer data returned.")
        return

    filtered_retainer_df = df_retainer[
        (df_retainer["RETAINER_STATE"] == "Open") &
        (~df_retainer["NAME"].str.contains("Flume", case=False, na=False)) &
        (~df_retainer["NAME"].str.contains("Elaine", case=False, na=False)) &
        (~df_retainer["NAME"].str.contains("Lee", case=False, na=False))
    ].copy()

    filtered_retainer_df["USEDVALUE"] = pd.to_numeric(
        filtered_retainer_df["USEDVALUE"], errors="coerce"
    )

    filtered_retainer_df["INVOICEDVALUE"] = pd.to_numeric(
        filtered_retainer_df["INVOICEDVALUE"], errors="coerce"
    )

    filtered_retainer_df["UTILIZATION"] = (
        filtered_retainer_df["USEDVALUE"] /
        filtered_retainer_df["INVOICEDVALUE"]
    ).replace([np.inf, -np.inf], np.nan).fillna(0) * 100

    # BUILD MESSAGES

    message_50 = build_message(
        filtered_retainer_df,
        50,
        80,
        "The following retainer deposits have now reached *50%* of their budget."
    )

    message_80 = build_message(
        filtered_retainer_df,
        80,
        100,
        "The following retainer deposits have now *surpassed 80%* of their budget."
    )

    # SEND OR PREVIEW

    send_message_as_user(
        channel_id=channel_id,
        message=message_50,
        user_token=slack_token,
        test_mode=TEST_MODE
    )

    send_message_as_user(
        channel_id=channel_id,
        message=message_80,
        user_token=slack_token,
        test_mode=TEST_MODE
    )

summary_message = f"""
✅ *Retainer Alerts Sent Successfully*

🟡 50% Threshold ({len(clients_50)} clients):
{chr(10).join(clients_50) if clients_50 else 'None'}

🔴 80% Threshold ({len(clients_80)} clients):
{chr(10).join(clients_80) if clients_80 else 'None'}
"""

send_message_as_user(
    channel_id=channel_id,
    message=summary_message,
    user_token=slack_token,
    test_mode=TEST_MODE
)

# RUN SCRIPT

if __name__ == "__main__":

    print("Starting Retainer Monitoring Job")

    run_retainer_job()

    print("✅ Job finished")
