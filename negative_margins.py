import os
import requests
import pandas as pd
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from xml.etree import ElementTree as ET
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

TEST_MODE = False

load_dotenv()

USER_URL = os.getenv("user_url") or os.getenv("user_url")
INVOICES_URL = os.getenv("invoices_url") or os.getenv("invoices_url")
USERNAME = os.getenv("username") or os.getenv("username")
PASSWORD = os.getenv("password") or os.getenv("password")
SLACK_TOKEN = os.getenv("slack_token") or os.getenv("slack_token")
MARGINS_URL = os.getenv("margins_url") or os.getenv("margins_url")

def get_odata_dataframe(url, username, password):
    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password), timeout=30)
        response.raise_for_status()
        data = response.json()

        if "value" in data:
            df = pd.DataFrame(data["value"])
            print("✅ Data successfully loaded into DataFrame")
            return df

        raise ValueError("No 'value' key found in JSON response")
    except Exception as e:
        print(f"❌ Error loading JSON OData: {e}")
        return pd.DataFrame()


def get_odata_dataframe_xml(url, username, password):
    try:
        headers = {"Accept": "application/atom+xml"}
        response = requests.get(
            url,
            auth=HTTPBasicAuth(username, password),
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()

        root = ET.fromstring(response.content)

        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
            "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
        }

        entries = []
        for entry in root.findall("atom:entry", ns):
            content = entry.find("atom:content", ns)
            properties = content.find("m:properties", ns)
            row = {}
            for prop in properties:
                tag = prop.tag.split("}")[-1]
                row[tag] = prop.text
            entries.append(row)

        df = pd.DataFrame(entries)
        print(f"✅ Parsed {len(df)} records into DataFrame")
        return df

    except Exception as e:
        print(f"❌ Error parsing XML: {e}")
        return pd.DataFrame()


def fetch_slack_users(slack_token):
    headers = {"Authorization": f"Bearer {slack_token}"}
    url = "https://slack.com/api/users.list"

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data.get("ok"):
            raise Exception(f"Slack API Error: {data.get('error')}")

        users = [
            {
                "user_id": member["id"],
                "real_name": member.get("real_name", ""),
                "email": member.get("profile", {}).get("email", ""),
                "job_title": member.get("profile", {}).get("title", ""),
                "is_bot": member.get("is_bot", False),
                "deleted": member.get("deleted", False),
            }
            for member in data["members"]
        ]

        df = pd.DataFrame(users)
        print("✅ Slack users pulled into DataFrame")
        return df

    except Exception as e:
        print(f"❌ Error fetching Slack users: {e}")
        return pd.DataFrame()


def open_dm_channel(client, user_id: str) -> str:
    resp = client.conversations_open(users=[user_id])
    return resp["channel"]["id"]


def notify_users_and_owners_margins(filtered_df, slack_token, test_mode=True):
    print("🔧 TEST MODE:", "ON (no Slack messages will be sent)" if test_mode else "OFF")
    client = WebClient(token=slack_token)

    df = filtered_df.copy()

    required_cols = ["user_slack_id", "user_email", "NAME"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"filtered_df missing required column(s): {missing}")

    df = df.dropna(subset=["user_slack_id"])
    df["user_slack_id"] = df["user_slack_id"].astype(str).str.strip()
    df["user_email"] = df["user_email"].astype(str).str.strip()
    df["NAME"] = df["NAME"].astype(str).str.strip()

    df = df.drop_duplicates(subset=["user_slack_id", "user_email", "NAME"])

    grouped = (
        df.groupby(["user_slack_id", "user_email"], dropna=False)
        .agg(projects=("NAME", list))
        .reset_index()
    )

    for _, row in grouped.iterrows():
        user_id = row["user_slack_id"]
        user_email = row["user_email"]
        projects = row["projects"]

        project_lines = "\n".join([f"• *{p}*" for p in projects])

        message = (
            f"Hi <@{user_id}> 👋\n\n"
            f"The following project(s) are currently in a *negative margin*:\n\n"
            f"{project_lines}\n\n"
            f"Please leave a comment on the home page explaining the context "
            f"for why the above has a negative margin.\n\n"
            f"Thank you 💙"
        )

        if test_mode:
            print(f"🧪 TEST → Would DM {user_email} ({user_id}):\n{message}\n")
            continue

        try:
            channel_id = open_dm_channel(client, user_id)
            client.chat_postMessage(channel=channel_id, text=message)
            print(f"✅ Message sent to {user_email}")

        except SlackApiError as e:
            err = e.response.get("error", "unknown_error")
            meta = e.response.get("response_metadata", {}) or {}
            needed = meta.get("needed")
            print(f"❌ Error sending to {user_email}: {err}" + (f" (needed: {needed})" if needed else ""))

        except Exception as e:
            print(f"❌ Unexpected error for {user_email}: {e}")


def main():
    if not all([USER_URL, MARGINS_URL, USERNAME, PASSWORD, SLACK_TOKEN]):
        print("❌ Missing one or more required environment variables.")
        return

    df_user = get_odata_dataframe(USER_URL, USERNAME, PASSWORD)
    df_margin = get_odata_dataframe_xml(MARGINS_URL, USERNAME, PASSWORD)
    df_slack_users = fetch_slack_users(SLACK_TOKEN)

    if df_user.empty or df_margin.empty or df_slack_users.empty:
        print("❌ One or more source dataframes are empty.")
        return

    df_margin["CUBE_PROJECT_MARGIN"] = pd.to_numeric(
        df_margin["CUBE_PROJECT_MARGIN"], errors="coerce"
    )

    filtered_margin_df = df_margin[
        (df_margin["CUBE_PROJECT_MARGIN"] < 0)
        & (df_margin["STATE"] == "Open")
        & (~df_margin["NAME"].str.contains("Flume", case=False, na=False))
    ].copy()

    df_slack = df_slack_users[
        (~df_slack_users["is_bot"]) & (~df_slack_users["deleted"])
    ].copy()
    df_slack["email"] = df_slack["email"].astype(str).str.lower().str.strip()

    df_user["email"] = df_user["DIVISION_OWNER_MAIL"].astype(str).str.lower().str.strip()
    merged_user = pd.merge(df_user, df_slack, on="email", how="left", suffixes=("", "_owner"))

    filtered_margin_df["email"] = filtered_margin_df["OWNER_MAIL"].astype(str).str.lower().str.strip()
    merged_margin = pd.merge(filtered_margin_df, df_slack, on="email", how="left", suffixes=("", "_user"))
    merged_margin["User"] = merged_margin["OWNER_MAIL"]

    df_combined_margin = pd.merge(merged_margin, merged_user, on="User", how="left")
    df_combined_margin = df_combined_margin.rename(
        columns={
            "user_id_x": "user_slack_id",
            "user_id_y": "owner_slack_id",
            "email_x": "user_email",
            "email_y": "owner_email",
        }
    )

    date_col = "CREATED_DT_y" if "CREATED_DT_y" in df_combined_margin.columns else "CREATED_DT"
    df_combined_margin[date_col] = pd.to_datetime(df_combined_margin[date_col], errors="coerce")
    cutoff = pd.Timestamp.today() - pd.Timedelta(days=120)
    filtered_margin = df_combined_margin[df_combined_margin[date_col] >= cutoff].copy()

    print(f"✅ Negative margin rows to notify: {len(filtered_margin)}")
    notify_users_and_owners_margins(filtered_margin, slack_token=SLACK_TOKEN, test_mode=TEST_MODE)


if __name__ == "__main__":
    print("🚀 Starting negative margins job")
    main()
    print("✅ Negative margins job finished")
