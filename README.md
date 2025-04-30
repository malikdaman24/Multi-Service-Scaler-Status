
# AWS Lambda for Dynamic Scaling and Status Validation (with Kubernetes Layer)

This repository contains two AWS Lambda functions to orchestrate and validate scaling of AWS ECS Services, Auto Scaling Groups (ASGs), and Kubernetes HPAs (on EKS) using a concurrency-driven CSV configuration file.

---

## 📌 Overview

1. **Scaling Lambda**
   - Parses a CSV from S3 using a concurrency key.
   - Scales ECS Services, ASGs, and EKS HPA (min/max/desired).
   - Supports fine-grained control via `include_resources`, `exclude_resources`, and `update_fields`.
   - Sends an SNS notification with the scaling summary.
   - Triggers a Step Function for post-scale validation.

2. **Status Checker Lambda**
   - Validates if the scaling actions succeeded after a wait period.
   - Checks ECS task counts, ASG instance states, and EKS HPA replicas.
   - Sends a detailed report (and failure alert if needed) via SNS.

---

## 🗂️ Project Structure

```plaintext
.
├── multi-service-scalar.py    # Main Lambda for applying scale
├── multi-service-status.py    # Lambda for verifying scale post-application
├── README.md                  # Project documentation
```

---

## ⚙️ Environment Variables

| Variable                    | Description                                              |
|-----------------------------|----------------------------------------------------------|
| `AWS_REGION`                | Default AWS region used for AWS SDK calls                |
| `S3_BUCKET`                 | Name of S3 bucket containing the CSV configuration       |
| `CSV_KEY`                   | S3 key (path) to the CSV file                            |
| `SNS_TOPIC_ARN`             | SNS topic ARN for success/failure notifications          |
| `SNS_TOPIC_ARN_FAILURE`     | (optional) Separate SNS ARN for failure-specific alerts  |
| `STEP_FUNCTION_ARN_STATUS`  | ARN of Step Function for status validation post-scaling  |

---

## 📄 CSV Format

CSV must contain a `concurrency` column and scaling keys as:

- **ECS Services**: `ecs:<region>:<cluster>#<service>_desired`, `_min`, `_max`
- **ASGs**: `asg:<region>:<asg>_desired`, `_min`, `_max`
- **EKS HPA**: `eks:<region>:<cluster>:<namespace>#<hpa>_min`, `_max`

Each row represents a configuration for a specific concurrency level.

---

## 🚀 Lambda Input Format

### Example Input for `scale-handler`

```json
{
  "concurrency": 10,
  "update_fields": "desired,min,max",
  "include_resources": "ecs:us-west-2:cluster#service",
  "exclude_resources": "asg:us-west-2:canary-asg"
}
```

---

## 🧪 Sample Output

```json
{
  "status": "success",
  "message": "Scaling updates applied for concurrency: 10",
  "summary": {
    "ecs": { "success": [...], "failed": [...] },
    "asg": { "success": [...], "failed": [...] },
    "eks": { "success": [...], "failed": [...] }
  }
}
```

---

## 📬 Notifications

SNS messages sent by both Lambdas include:

- Original Lambda event
- CSV configuration row used
- Summary of success/failures for each resource type

---

## ☸️ Kubernetes Layer for EKS HPA Access

To enable the Lambda to interact with the Kubernetes API for updating HPAs, follow these steps:

### 1. Create a Lambda Layer

Package the Kubernetes client libraries and dependencies into a layer.

```bash
pip install kubernetes -t python
zip -r kubernetes-layer.zip python
```

### 2. Publish the Layer

```bash
aws lambda publish-layer-version   --layer-name eks-k8s-client   --zip-file fileb://kubernetes-layer.zip   --compatible-runtimes python3.9
```

### 3. Attach the Layer to Lambda

Update your Lambda configuration to include the ARN of the published layer.

---

### IAM Permissions for Lambda Role

Ensure your Lambda has the following minimum permissions:

- `eks:DescribeCluster`
- `sts:GetCallerIdentity`
- `autoscaling:*`
- `ecs:*`
- `application-autoscaling:*`
- `sns:Publish`
- `s3:GetObject`
- `states:StartExecution` (for Step Function)

---

## 🔐 Security Considerations

- Uses temporary AWS STS tokens for EKS authentication.
- SSL certificate from cluster is extracted and stored at `/tmp/ca.crt`.
- No hardcoded secrets; all values fetched from environment or STS.

---

## 🧩 Extending

- Add support for additional resource types by extending the handlers.
- Schedule periodic audits using CloudWatch Events.
- Integrate Slack/Teams notifications via Lambda-to-webhook mapping.

---

## 🧼 Best Practices

- Avoid full-scale updates for each change — use `include_resources` smartly.
- Monitor Lambda duration and retry behavior for long-running updates.

---