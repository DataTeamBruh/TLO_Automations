import os
import requests
import pandas as pd
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from xml.etree import ElementTree as ET
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


TEST_MODE = True # Set to True to prevent actual Slack messages during testing

load_dotenv()

INVOICES_URL = os.getenv("invoices_url")
USERNAME = os.getenv("username")
PASSWORD = os.getenv("password")
SLACK_TOKEN = os.getenv("slack_token")


REQUIRED_INVOICE_COLUMNS = [
    "INVOICE_NUMBER",
    "NAME",
    "INVOICE_STATE",
    "INVOICE_OWNER_MAIL",
    "INVOICE_INVOICE_DT",
]


def get_odata_dataframe_xml(url, username, password):
    """
    Fetch invoice data from an XML OData endpoint and convert it into a DataFrame.
    Keeps only the invoice columns needed for this job.
    """
    try:
        headers = {"Accept": "application/atom+xml"}

        response = requests.get(
            url,
            auth=HTTPBasicAuth(username, password),
            headers=headers,
            timeout=60,
        )
        response.raise_for_status()

        root = ET.fromstring(response.content)

        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
            "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
        }

        rows = []

        for entry in root.findall("atom:entry", ns):
            content = entry.find("atom:content", ns)
            if content is None:
                continue

            properties = content.find("m:properties", ns)
            if properties is None:
                continue

            row = {}

            for prop in properties:
                column_name = prop.tag.split("}")[-1]

                if column_name in REQUIRED_INVOICE_COLUMNS:
                    row[column_name] = prop.text

            if row:
                rows.append(row)

        df = pd.DataFrame(rows)

        print(f"✅ Parsed {len(df)} invoice records into DataFrame")
        return df

    except Exception as e:
        print(f"❌ Error parsing XML invoice data: {e}")
        return pd.DataFrame()


def fetch_slack_users(slack_token):
    """
    Pull active Slack users and return email-to-user-id mapping.
    Includes pagination.
    """
    client = WebClient(token=slack_token)

    users = []
    cursor = None

    try:
        while True:
            response = client.users_list(cursor=cursor, limit=200)

            if not response.get("ok"):
                raise Exception(f"Slack API Error: {response.get('error')}")

            members = response.get("members", [])

            for member in members:
                profile = member.get("profile", {}) or {}

                users.append(
                    {
                        "user_id": member.get("id"),
                        "email": profile.get("email", ""),
                        "is_bot": member.get("is_bot", False),
                        "deleted": member.get("deleted", False),
                    }
                )

            cursor = response.get("response_metadata", {}).get("next_cursor")

            if not cursor:
                break

        df = pd.DataFrame(users)

        if df.empty:
            print("❌ Slack users DataFrame is empty")
            return df

        df = df[
            (~df["is_bot"]) &
            (~df["deleted"]) &
            (df["email"].notna()) &
            (df["email"] != "")
        ][["email", "user_id"]].copy()

        df["email"] = df["email"].astype(str).str.lower().str.strip()

        print(f"✅ Pulled {len(df)} active Slack users")
        return df

    except Exception as e:
        print(f"❌ Error fetching Slack users: {e}")
        return pd.DataFrame()


def prepare_overdue_invoices(df_invoice):
    """
    Filter invoice data to only overdue invoices from the last 120 days.
    """
    if df_invoice.empty:
        return pd.DataFrame()

    missing_columns = [
        col for col in REQUIRED_INVOICE_COLUMNS if col not in df_invoice.columns
    ]

    if missing_columns:
        print(f"❌ Missing invoice columns: {missing_columns}")
        return pd.DataFrame()

    df = df_invoice[REQUIRED_INVOICE_COLUMNS].copy()

    df["INVOICE_STATE"] = df["INVOICE_STATE"].astype(str).str.strip()
    df = df[df["INVOICE_STATE"].eq("Overdue")].copy()

    df["INVOICE_INVOICE_DT"] = pd.to_datetime(
        df["INVOICE_INVOICE_DT"],
        errors="coerce",
    )

    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=120)

    df = df[
        df["INVOICE_INVOICE_DT"].notna() &
        (df["INVOICE_INVOICE_DT"] >= cutoff)
    ].copy()

    df["user_email"] = (
        df["INVOICE_OWNER_MAIL"]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    df["INVOICE_NUMBER"] = df["INVOICE_NUMBER"].astype(str).str.strip()
    df["NAME"] = df["NAME"].astype(str).str.strip()

    df = df[
        ["user_email", "INVOICE_NUMBER", "NAME", "INVOICE_INVOICE_DT"]
    ].copy()

    print(f"✅ Overdue invoices after filtering: {len(df)}")
    return df


def attach_slack_ids(df_overdue, df_slack_users):
    """
    Add Slack user IDs to overdue invoice rows by matching invoice owner email.
    """
    if df_overdue.empty or df_slack_users.empty:
        return pd.DataFrame()

    df = pd.merge(
        df_overdue,
        df_slack_users,
        left_on="user_email",
        right_on="email",
        how="left",
    )

    df = df.rename(columns={"user_id": "user_slack_id"})

    df = df[
        ["user_email", "user_slack_id", "INVOICE_NUMBER", "NAME", "INVOICE_INVOICE_DT"]
    ].copy()

    missing_slack = df["user_slack_id"].isna().sum()

    if missing_slack:
        print(f"⚠️ {missing_slack} overdue invoice rows have no matching Slack user")

    df = df.dropna(subset=["user_slack_id"]).copy()

    df["user_slack_id"] = df["user_slack_id"].astype(str).str.strip()

    df = df.drop_duplicates(
        subset=["user_slack_id", "user_email", "INVOICE_NUMBER", "NAME"]
    )

    print(f"✅ Overdue invoice rows ready for Slack notification: {len(df)}")
    return df


def open_dm_channel(client, user_id):
    response = client.conversations_open(users=[user_id])
    return response["channel"]["id"]


def notify_users_overdue(df_notifications, slack_token, test_mode=True):
    """
    Group invoices by Slack user and send one DM per user.
    Returns a list of recipients who were messaged,
    or who would be messaged in test mode.
    """
    print("🔧 TEST MODE:", "ON - no Slack messages will be sent" if test_mode else "OFF")

    sent_recipients = []

    if df_notifications.empty:
        print("ℹ️ No overdue invoices to notify")
        return sent_recipients

    client = WebClient(token=slack_token)

    grouped = (
        df_notifications
        .groupby(["user_slack_id", "user_email"], dropna=False)
        .agg(
            invoice_numbers=("INVOICE_NUMBER", list),
            entity_names=("NAME", list),
        )
        .reset_index()
    )

    print(f"✅ Users to notify: {len(grouped)}")

    for _, row in grouped.iterrows():
        user_id = row["user_slack_id"]
        user_email = row["user_email"]
        invoices = row["invoice_numbers"]
        entities = row["entity_names"]

        seen = set()
        lines = []

        for invoice_number, entity_name in zip(invoices, entities):
            key = (invoice_number, entity_name)

            if key in seen:
                continue

            seen.add(key)
            lines.append(f"• *{invoice_number}* — {entity_name}")

        invoice_block = "\n".join(lines) if lines else "• No invoice details found"

        message = (
            f"Hi <@{user_id}> 👋\n\n"
            f"The following invoice(s) are still outstanding:\n\n"
            f"{invoice_block}\n\n"
            f"Could you kindly touch base with the client to confirm when payment is expected?\n\n"
            f"Thanks so much for helping keep things on track 💙"
        )

        recipient_record = {
            "user_email": user_email,
            "user_slack_id": user_id,
            "invoice_count": len(lines),
        }

        if test_mode:
            print(f"🧪 TEST → Would DM {user_email} ({user_id}):\n{message}\n")
            sent_recipients.append(recipient_record)
            continue

        try:
            channel_id = open_dm_channel(client, user_id)
            client.chat_postMessage(channel=channel_id, text=message)
            print(f"✅ Message sent to {user_email}")
            sent_recipients.append(recipient_record)

        except SlackApiError as e:
            error = e.response.get("error", "unknown_error")
            metadata = e.response.get("response_metadata", {}) or {}
            needed = metadata.get("needed")

            print(
                f"❌ Error sending to {user_email}: {error}"
                + (f" - needed: {needed}" if needed else "")
            )

        except Exception as e:
            print(f"❌ Unexpected error sending to {user_email}: {e}")

    return sent_recipients


def print_sent_recipients(sent_recipients):
    """
    Print the final list of people messaged after the job finishes.
    """
    if sent_recipients:
        print(
            "📬 Messages sent to:"
            if not TEST_MODE
            else "📬 TEST MODE - messages would be sent to:"
        )

        for recipient in sent_recipients:
            print(
                f"- {recipient['user_email']} "
                f"({recipient['user_slack_id']}) "
                f"- {recipient['invoice_count']} invoice(s)"
            )
    else:
        print("📬 No Slack messages were sent.")


def main():
    if not all([INVOICES_URL, USERNAME, PASSWORD, SLACK_TOKEN]):
        print("❌ Missing one or more required environment variables.")
        return

    print("🚀 Starting overdue accounts job")

    df_invoice = get_odata_dataframe_xml(
        INVOICES_URL,
        USERNAME,
        PASSWORD,
    )

    if df_invoice.empty:
        print("❌ Invoice data is empty. Stopping job.")
        print("✅ Overdue accounts job finished")
        print("📬 No Slack messages were sent.")
        return

    df_overdue = prepare_overdue_invoices(df_invoice)

    if df_overdue.empty:
        print("ℹ️ No overdue invoices found in the last 120 days.")
        print("✅ Overdue accounts job finished")
        print("📬 No Slack messages were sent.")
        return

    df_slack_users = fetch_slack_users(SLACK_TOKEN)

    if df_slack_users.empty:
        print("❌ Slack users data is empty. Stopping job.")
        print("✅ Overdue accounts job finished")
        print("📬 No Slack messages were sent.")
        return

    df_notifications = attach_slack_ids(df_overdue, df_slack_users)

    if df_notifications.empty:
        print("ℹ️ No overdue invoices matched to Slack users.")
        print("✅ Overdue accounts job finished")
        print("📬 No Slack messages were sent.")
        return

    sent_recipients = notify_users_overdue(
        df_notifications,
        slack_token=SLACK_TOKEN,
        test_mode=TEST_MODE,
    )

    print("✅ Overdue accounts job finished")
    print_sent_recipients(sent_recipients)


if __name__ == "__main__":
    main()
