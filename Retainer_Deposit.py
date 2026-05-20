import os
import datetime
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth


# CONFIG
TEST_MODE = False  # Set to True to preview messages without sending to Slack

load_dotenv()

RETAINER_URL = os.getenv("retainer_url")
USERNAME = os.getenv("username")
PASSWORD = os.getenv("password")
CHANNEL_ID = os.getenv("channel_id")
SLACK_TOKEN = os.getenv("slack_token")


REQUIRED_RETAINER_COLUMNS = [
    "RETAINER_STATE",
    "NAME",
    "ENTITY_CLIENT_NAME",
    "USEDVALUE",
    "INVOICEDVALUE",
]


def get_odata_dataframe(url, username, password):
    """
    Fetch retainer data from a JSON OData endpoint and convert it into a DataFrame.
    Keeps only the columns needed for this job.
    """
    try:
        response = requests.get(
            url,
            auth=HTTPBasicAuth(username, password),
            timeout=60,
        )

        response.raise_for_status()
        data = response.json()

        if "value" not in data:
            raise ValueError("No 'value' key found in JSON response")

        rows = data["value"]
        df = pd.DataFrame(rows)

        if df.empty:
            print("⚠️ OData response returned no rows")
            return df

        available_columns = [
            col for col in REQUIRED_RETAINER_COLUMNS if col in df.columns
        ]

        df = df[available_columns].copy()

        print(f"✅ Retainer data loaded into DataFrame: {len(df)} rows")
        return df

    except Exception as e:
        print(f"❌ Error loading retainer OData: {e}")
        return pd.DataFrame()


def prepare_retainer_data(df_retainer):
    """
    Filter open retainers, exclude internal/test retainers, and calculate utilization.
    """
    if df_retainer.empty:
        return pd.DataFrame()

    missing_columns = [
        col for col in REQUIRED_RETAINER_COLUMNS if col not in df_retainer.columns
    ]

    if missing_columns:
        print(f"❌ Missing retainer columns: {missing_columns}")
        return pd.DataFrame()

    df = df_retainer[REQUIRED_RETAINER_COLUMNS].copy()

    df["RETAINER_STATE"] = df["RETAINER_STATE"].astype(str).str.strip()
    df["NAME"] = df["NAME"].astype(str).str.strip()
    df["ENTITY_CLIENT_NAME"] = df["ENTITY_CLIENT_NAME"].astype(str).str.strip()

    df["USEDVALUE"] = pd.to_numeric(
        df["USEDVALUE"],
        errors="coerce",
    )

    df["INVOICEDVALUE"] = pd.to_numeric(
        df["INVOICEDVALUE"],
        errors="coerce",
    )

    df = df[
        (df["RETAINER_STATE"].eq("Open"))
        & (~df["NAME"].str.contains("Flume", case=False, na=False))
        & (~df["NAME"].str.contains("Elaine", case=False, na=False))
        & (~df["NAME"].str.contains("Lee", case=False, na=False))
    ].copy()

    df["UTILIZATION"] = np.where(
        df["INVOICEDVALUE"] > 0,
        (df["USEDVALUE"] / df["INVOICEDVALUE"]) * 100,
        0,
    )

    df["UTILIZATION"] = (
        df["UTILIZATION"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    print(f"✅ Open retainers after filtering: {len(df)}")
    return df


def build_message(df, lower_bound, upper_bound, title):
    """
    Build a Slack message for retainers within a utilization threshold.
    """
    today = datetime.date.today()

    df_check = df[
        (df["UTILIZATION"] >= lower_bound)
        & (df["UTILIZATION"] < upper_bound)
    ].copy()

    message = (
        f"{title}"
        "\nPlease keep an eye on them to help prevent any overspending."
        "\nStaying on top of these ensures everything runs smoothly.\n\n"
        f"As at {today.strftime('%A, %d %B %Y')}:\n"
    )

    if df_check.empty:
        message += "\n_No retainers matched this threshold._"
        return message, 0

    df_check = df_check.sort_values(
        by="UTILIZATION",
        ascending=False,
    )

    lines = []

    for _, row in df_check.iterrows():
        client_name = row.get("ENTITY_CLIENT_NAME", "Unknown client")
        retainer_name = row.get("NAME", "Unknown retainer")

        util = row.get("UTILIZATION", 0)
        used = row.get("USEDVALUE", 0)
        budget = row.get("INVOICEDVALUE", 0)

        lines.append(
            f"• {client_name}, {retainer_name}: *{util:.1f}%* used "
            f"(R{used:,.0f} of R{budget:,.0f})"
        )

    message += "\n".join(lines)

    return message, len(df_check)


def send_message_to_slack(channel_id, message, slack_token, label, matched_count, test_mode=True):
    """
    Send or preview a Slack message.
    Returns a send result dictionary for the final job summary.
    """
    result = {
        "label": label,
        "channel_id": channel_id,
        "matched_count": matched_count,
        "sent": False,
        "test_mode": test_mode,
        "error": None,
    }

    if test_mode:
        print("\n🧪 TEST MODE — message NOT sent to Slack\n")
        print(f"Channel: {channel_id}")
        print(f"Message type: {label}")
        print(message)
        print("-" * 80)

        result["sent"] = True
        return result

    url = "https://slack.com/api/chat.postMessage"

    headers = {
        "Authorization": f"Bearer {slack_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "channel": channel_id,
        "text": message,
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30,
        )

        response_data = response.json()

        if response.status_code == 200 and response_data.get("ok"):
            print(f"✅ {label} message sent successfully")
            result["sent"] = True
            return result

        error_message = response_data.get("error", response.text)
        print(f"❌ Failed to send {label} message: {error_message}")

        result["error"] = error_message
        return result

    except Exception as e:
        print(f"❌ Unexpected error sending {label} message: {e}")
        result["error"] = str(e)
        return result


def print_job_summary(send_results):
    """
    Print final summary after the job finishes.
    """
    if not send_results:
        print("📬 No Slack messages were prepared.")
        return

    print("📬 Slack message summary:")

    for result in send_results:
        status = "sent" if result["sent"] and not result["test_mode"] else "previewed"
        if result["error"]:
            status = f"failed - {result['error']}"

        print(
            f"- {result['label']}: {status} to channel "
            f"{result['channel_id']} "
            f"({result['matched_count']} retainer(s) matched)"
        )


def run_retainer_job():
    """
    Main retainer monitoring job.
    """
    if not all([RETAINER_URL, USERNAME, PASSWORD, CHANNEL_ID, SLACK_TOKEN]):
        print("❌ Missing one or more required environment variables.")
        return []

    df_retainer = get_odata_dataframe(
        RETAINER_URL,
        USERNAME,
        PASSWORD,
    )

    if df_retainer.empty:
        print("⚠️ No retainer data returned.")
        return []

    filtered_retainer_df = prepare_retainer_data(df_retainer)

    if filtered_retainer_df.empty:
        print("ℹ️ No open retainers available after filtering.")
        return []

    message_50, count_50 = build_message(
        filtered_retainer_df,
        50,
        80,
        "The following retainer deposits have now reached *50%* of their budget.",
    )

    message_80, count_80 = build_message(
        filtered_retainer_df,
        80,
        100,
        "The following retainer deposits have now *surpassed 80%* of their budget.",
    )

    send_results = []

    send_results.append(
        send_message_to_slack(
            channel_id=CHANNEL_ID,
            message=message_50,
            slack_token=SLACK_TOKEN,
            label="50% retainer threshold",
            matched_count=count_50,
            test_mode=TEST_MODE,
        )
    )

    send_results.append(
        send_message_to_slack(
            channel_id=CHANNEL_ID,
            message=message_80,
            slack_token=SLACK_TOKEN,
            label="80% retainer threshold",
            matched_count=count_80,
            test_mode=TEST_MODE,
        )
    )

    return send_results


if __name__ == "__main__":
    print("🚀 Starting Retainer Monitoring Job")

    results = run_retainer_job()

    print("✅ Job finished")
    print_job_summary(results)
