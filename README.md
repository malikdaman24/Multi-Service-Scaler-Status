
# AWS Lambda for Dynamic Scaling and Status Validation

This repository contains two AWS Lambda functions designed to manage and validate scaling configurations for ECS Services, Auto Scaling Groups (ASGs), and EKS HPAs based on a concurrency-driven CSV configuration.

---

## 📌 Overview

1. **Scaling Lambda (`scale-handler`)**
   - Dynamically adjusts ECS services, ASGs, and EKS HPA parameters (`min`, `max`, `desired`) based on concurrency values.
   - Configurations are fetched from a CSV, it can be stored in S3.
   - Optionally filters resources using include/exclude lists.
   - Sends scaling summaries via SNS.
   - Optionally triggers a Step Function to run a post-scale verification.

2. **Status Checker Lambda (`status-verifier`)**
   - Triggered by the Step Function after a wait period (e.g., 30 minutes).
   - Checks whether the desired scaling was successfully applied.
   - Sends detailed success/failure summaries via SNS.

---

## 🗂️ Folder Structure

```plaintext
.
├── scale_handler.py           # Main scaling Lambda
├── status_checker.py         # Post-scaling status validation Lambda
├── README.md                 # This documentation
```

---

## ⚙️ Environment Variables

| Variable               | Description                                              |
|------------------------|----------------------------------------------------------|
| `AWS_REGION`           | Default AWS region for service calls                     |
| `S3_BUCKET`            | S3 bucket where the CSV config is stored                 |
| `CSV_KEY`              | S3 key (path) to the CSV file                            |
| `SNS_TOPIC_ARN`        | SNS topic ARN for notifications                          |
| `SNS_TOPIC_ARN_FAILURE`| (optional) SNS ARN for sending only failure alerts       |
| `STEP_FUNCTION_ARN_STATUS` | ARN of the Step Function triggered for status validation |

---

## 📄 CSV Format

CSV must contain a `concurrency` column and multiple scaling keys in the format:

- ECS: `ecs:<region>:<cluster>#<service>_min`, `_max`, `_desired`
- ASG: `asg:<region>:<asg>_min`, `_max`, `_desired`
- EKS HPA: `eks:<region>:<cluster>:<namespace>#<hpa>_min`, `_max`

---

## 🚀 Trigger Input Format

### Scaling Lambda Input

```json
{
  "concurrency": 10,
  "update_fields": "desired,min,max",
  "include_resources": "ecs:us-west-2:cluster#svc,asg:us-west-2:asg1",
  "exclude_resources": "eks:us-west-2:cluster:ns#hpa"
}
```

### Output

```json
{
  "status": "success",
  "message": "...",
  "summary": {
    "ecs": { "success": [...], "failed": [...] },
    "asg": { "success": [...], "failed": [...] },
    "eks": { "success": [...], "failed": [...] }
  }
}
```

---

## ✅ Status Checker Output

Includes actual replica counts and comparisons for ECS, ASG, and HPA resources against their `min` thresholds.

---

## 📬 Notifications

SNS messages will contain:

- Lambda event input
- Selected CSV row
- Execution summary

If failures are detected, an additional SNS is sent to `SNS_TOPIC_ARN_FAILURE`.

---

## 🛡️ Security

- Temporary token-based EKS access via STS
- CA cert extracted to `/tmp/ca.crt` for Kubernetes client validation

---

## 🧪 Testing & Deployment

- Lambda timeouts should allow time for retries and failover handling.

---
