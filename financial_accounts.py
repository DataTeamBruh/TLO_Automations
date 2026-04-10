import os
import requests
import pandas as pd
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from xml.etree import ElementTree as ET
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

TEST_MODE = True

load_dotenv()

USER_URL = os.getenv("user_url") or os.getenv("user_url")
INVOICES_URL = os.getenv("invoices_url") or os.getenv("invoices_url")
USERNAME = os.getenv("username") or os.getenv("username")
PASSWORD = os.getenv("password") or os.getenv("password")
SLACK_TOKEN = os.getenv("slack_token") or os.getenv("slack_token")


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


def notify_users_about_invoices(filtered_df: pd.DataFrame, slack_token: str, preview: bool = True) -> None:
    client = WebClient(token=slack_token)

    required_cols = ["user_slack_id", "user_email", "INVOICE_NUMBER"]
    missing = [c for c in required_cols if c not in filtered_df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    base_df = filtered_df.dropna(subset=["user_slack_id", "INVOICE_NUMBER"]).copy()
    deduped_df = base_df.drop_duplicates(subset=["user_slack_id", "INVOICE_NUMBER"])

    if preview:
        removed = len(base_df) - len(deduped_df)
        if removed > 0:
            print(
                f"ℹ️ Deduplicated messages: {removed} duplicates removed "
                f"(from {len(base_df)} rows to {len(deduped_df)} unique messages)."
            )

    for _, row in deduped_df.iterrows():
        user_id = str(row.get("user_slack_id")).strip()
        user_email = str(row.get("user_email", "")).strip()
        invoice_num = str(row.get("INVOICE_NUMBER", "")).strip()

        user_msg = (
            f"Hi <@{user_id}>, 😊\n\n"
            f"Just a quick reminder to double-check all the line items on *{invoice_num}*. "
            f"A few of them are still linked to the 4045 default financial account. "
            f"When you get a moment, please update your proforma with the right departmental "
            f"fin accounts before ticking the invoice check box for Alexia to commit.\n\n"
            f"This will help prevent misallocation of departmental income.\n\n"
            f"Thank you 💙"
        )

        if preview:
            print("🧪 PREVIEW MODE — message NOT sent")
            print(f"To: {user_email} (Slack ID: {user_id})")
            print(user_msg)
            print("-" * 80)
            continue

        try:
            client.chat_postMessage(channel=user_id, text=user_msg)
            print(f"✅ Invoice reminder sent to {user_email} (user_id: {user_id})")
        except SlackApiError as e:
            err = e.response.get("error", "unknown_error")
            print(f"❌ Failed to send to {user_email} (user_id: {user_id}): {err}")
        except Exception as e:
            print(f"❌ Unexpected error sending to {user_email} (user_id: {user_id}): {e}")


def main():
    if not all([USER_URL, INVOICES_URL, USERNAME, PASSWORD, SLACK_TOKEN]):
        print("❌ Missing one or more required environment variables.")
        return

    df_user = get_odata_dataframe(USER_URL, USERNAME, PASSWORD)
    df_invoice = get_odata_dataframe_xml(INVOICES_URL, USERNAME, PASSWORD)
    df_slack_users = fetch_slack_users(SLACK_TOKEN)

    if df_user.empty or df_invoice.empty or df_slack_users.empty:
        print("❌ One or more source dataframes are empty.")
        return

    df_invoice_fin = df_invoice[
        (df_invoice["FINACCOUNT_CODE"] == "4045")
        & (df_invoice["INVOICE_NUMBER"].str.contains("PF", case=False, na=False))
    ].copy()

    df_slack = df_slack_users[
        (~df_slack_users["is_bot"]) & (~df_slack_users["deleted"])
    ].copy()
    df_slack["email"] = df_slack["email"].astype(str).str.lower().str.strip()

    df_user["email"] = df_user["DIVISION_OWNER_MAIL"].astype(str).str.lower().str.strip()
    merged_user = pd.merge(df_user, df_slack, on="email", how="left", suffixes=("", "_owner"))

    df_invoice_fin["email"] = df_invoice_fin["INVOICE_OWNER_MAIL"].astype(str).str.lower().str.strip()
    merged_fin = pd.merge(df_invoice_fin, df_slack, on="email", how="left", suffixes=("", "_user"))
    merged_fin["User"] = merged_fin["INVOICE_OWNER_MAIL"]

    df_combined_fin = pd.merge(merged_fin, merged_user, on="User", how="left")
    df_combined_fin = df_combined_fin.rename(
        columns={
            "user_id_x": "user_slack_id",
            "user_id_y": "owner_slack_id",
            "email_x": "user_email",
            "email_y": "owner_email",
        }
    )

    date_col = "CREATED_DT_y" if "CREATED_DT_y" in df_combined_fin.columns else "CREATED_DT"
    df_combined_fin[date_col] = pd.to_datetime(df_combined_fin[date_col], errors="coerce")
    cutoff = pd.Timestamp.today() - pd.Timedelta(days=120)
    filtered_fin = df_combined_fin[df_combined_fin[date_col] >= cutoff].copy()

    print(f"✅ Financial accounts rows to notify: {len(filtered_fin)}")
    notify_users_about_invoices(filtered_fin, slack_token=SLACK_TOKEN, preview=TEST_MODE)


if __name__ == "__main__":
    print("🚀 Starting financial accounts job")
    main()
    print("✅ Financial accounts job finished")