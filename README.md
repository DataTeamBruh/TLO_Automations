# TLO_Invoice
Automation Project_TLO_Invoices
## Project Overview 
This project automates the extraction, processing, and distribution of financial data from TeamLeader Orbit (TLO) to relevant stakeholders via Slack. Currently, this process is run manually from a local Visual Studio Code environment, requiring a developer to be present to execute the scripts and distribute the outputs.
The objective is to migrate this workflow to a hosted, scheduled environment where the automation runs independently on a defined daily schedule - without manual intervention. The solution will also incorporate Healthchecks.io to provide real-time monitoring, alerting, and audit trails for every scheduled run.
## Problem Statement
The current process has the following limitations:
The automation scripts run only on a local machine (VS Code), creating a single point of failure dependent on one person's availability.
There is no scheduling mechanism - reports must be triggered manually each time.
There is no alerting or monitoring in place. If the script fails silently, stakeholders receive no notification and the issue may go undetected.
The process is not scalable or auditable - there is no centralised log of when reports were sent, to whom, or whether they succeeded.
## Aim
To automate the daily extraction and distribution of financial data from TeamLeader Orbit to Slack,
by migrating the existing local script to a hosted scheduled environment with full monitoring and alerting via Healthchecks.io,  eliminating manual effort and ensuring reliable, timely delivery to stakeholders.
## Objectives
Migrate the existing Python/VS Code automation scripts to a hosted server or cloud environment.
Implement a cron-based scheduler to run the data extraction and Slack notification pipeline at a defined time each day.
Ensure financial reports covering unsent invoices, account margins, account spending, and financial tracking reminders are delivered automatically to the correct Slack channels or recipients.
Integrate Healthchecks.io to monitor every scheduled run and send alerts when a job succeeds, is late, or fails.
Remove dependency on any individual's local machine for the process to execute.
Produce technical documentation and a runbook to support ongoing maintenance.
## Deliverables
Configured cron scheduler running daily at a defined time.
Slack notifications delivering: unsent invoice alerts, account margin summaries, account spending summaries, and financial tracking reminders.
Healthchecks.io monitoring setup with configured success, failure, and late-run alerts.
Technical documentation including environment setup guide and runbook.
## Methodology
The project will follow an iterative delivery approach, broken into clearly defined phases. Each phase will be completed and validated before the next begins, reducing the risk of rework.
8.1  Current State (As-Is)
Developer runs Python scripts locally in Visual Studio Code.
Scripts connect to the TeamLeader Orbit (TLO) API to pull financial data.
Processed data is pushed to Slack via webhook notifications.
Process is entirely manual - no scheduling, no monitoring, no alerts.
8.2  Future State (To-Be)
Scripts are hosted on a server or cloud platform (e.g. Linux VPS, AWS EC2, or similar).
A cron job triggers the script automatically every day at the defined run time.
Slack notifications are dispatched automatically to the relevant recipients.
Healthchecks.io receives a ping on each run - alerting the team on success, failure, or missed runs.
## Success Criteria
The automation runs daily at the scheduled time without manual intervention for a minimum of 10 consecutive business days post go-live.
All four financial notification types (unsent invoices, account margins, account spending, financial tracking reminders) are delivered accurately to the correct Slack recipients.
Healthchecks.io successfully detects and alerts on at least one simulated failure during UAT.
Zero dependency on any individual's local machine for the process to execute.
Finance stakeholder sign-off confirming report data is accurate and complete.
