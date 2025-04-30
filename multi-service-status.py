import boto3
import json
import logging
import os
import base64
import re
from kubernetes import client
from kubernetes.client.rest import ApiException
from botocore.signers import RequestSigner

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables (provided via Lambda configuration)
aws_region = os.environ.get("AWS_REGION", "<REGION>")
S3_BUCKET = os.environ.get("S3_BUCKET", "<YOUR-BUCKET-NAME>")
CSV_KEY = os.environ.get("CSV_KEY", "<CSV-FILE-NAME>")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")             # Primary SNS topic ARN for status notifications
SNS_TOPIC_ARN_FAILURE = os.environ.get("SNS_TOPIC_ARN_FAILURE") # SNS topic ARN for failure notifications

# Global clients (default region) – not used for region-specific checks.
global_ecs_client = boto3.client('ecs', region_name=aws_region)
global_asg_client = boto3.client('autoscaling', region_name=aws_region)

########################################################################
# Helper: Normalize canonical resource IDs for filtering
########################################################################
def normalize_resource_id(resource_id):
    """Return a lowercase, trimmed version of the resource ID for consistent filtering."""
    return resource_id.strip().lower()

########################################################################
# EKS Helper Functions
########################################################################
def get_bearer_token(target_cluster, region=None):
    """
    Generates an authentication token for a given EKS cluster using Botocore’s RequestSigner.
    
    Parameters:
      - target_cluster: Name of the target EKS cluster (must match the aws-auth ConfigMap).
      - region: AWS region where the cluster resides (defaults to aws_region).
      
    Returns a token string in the format:
      k8s-aws-v1.<base64url-encoded-signed-url>
    
    (Token expiration is set to 300 seconds.)
    """
    if region is None:
        region = aws_region
    STS_TOKEN_EXPIRES_IN = 300
    session = boto3.session.Session(region_name=region)
    sts_client = session.client('sts')
    service_id = sts_client.meta.service_model.service_id

    signer = RequestSigner(
        service_id,
        region,
        'sts',
        'v4',
        session.get_credentials(),
        session.events
    )

    params = {
        'method': 'GET',
        'url': f'https://sts.{region}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15',
        'body': {},
        'headers': {
            'x-k8s-aws-id': target_cluster
        },
        'context': {}
    }

    signed_url = signer.generate_presigned_url(
        params,
        region_name=region,
        expires_in=STS_TOKEN_EXPIRES_IN,
        operation_name=''
    )

    base64_url = base64.urlsafe_b64encode(signed_url.encode('utf-8')).decode('utf-8')
    token = 'k8s-aws-v1.' + re.sub(r'=*', '', base64_url)
    logger.info(f"Generated EKS bearer token for cluster '{target_cluster}' in region '{region}'.")
    return token

def get_eks_client_for_hpa(target_cluster, target_region=None):
    """
    Sets up and returns a Kubernetes AutoscalingV1Api client for updating EKS HPAs.
    
    Parameters:
      - target_cluster: Name of the target EKS cluster.
      - target_region: AWS region where the target cluster resides (defaults to aws_region).
      
    Returns an AutoscalingV1Api client configured with the target cluster's endpoint,
    CA certificate, and a valid token.
    """
    if target_region is None:
        target_region = aws_region
    try:
        eks = boto3.client('eks', region_name=target_region)
        cluster_info = eks.describe_cluster(name=target_cluster)
        cluster_endpoint = cluster_info['cluster']['endpoint']
        cert_authority = cluster_info['cluster']['certificateAuthority']['data']
        
        with open('/tmp/ca.crt', 'wb') as f:
            f.write(base64.b64decode(cert_authority))
        
        configuration = client.Configuration()
        configuration.host = cluster_endpoint
        configuration.verify_ssl = True
        configuration.ssl_ca_cert = '/tmp/ca.crt'
        configuration.api_key['authorization'] = get_bearer_token(target_cluster, target_region)
        configuration.api_key_prefix['authorization'] = 'Bearer'
        
        client.Configuration.set_default(configuration)
        logger.info(f"Kubernetes client configured for EKS cluster '{target_cluster}' in region '{target_region}'.")
        return client.AutoscalingV1Api(client.ApiClient(configuration))
    except Exception as e:
        logger.error(f"Failed to set up EKS client for cluster '{target_cluster}' in region '{target_region}': {e}")
        raise

########################################################################
# Main Lambda Handler: Status Check, Reporting, and SNS Notification
########################################################################
def lambda_handler(event, context):
    """
    This Lambda is triggered by a Step Function (after a 30-minute wait) to verify that scaling changes have been applied.
    
    Input from the Step Function includes:
      - "csv_config": The CSV configuration row used for scaling.
      - "scaling_summary": The summary from the scaling Lambda.
      - "concurrency": The concurrency value.
      - "original_event": The original Lambda event.
    
    For each resource type (ECS, ASG, and EKS HPA), this function:
      - Retrieves the current running state via the relevant API.
      - For ECS, it compares the 'runningCount' (the actual number of running tasks) with the configured minimum count.
      - For ASGs, it counts the number of instances with LifecycleState "InService" and compares that count with the configured minimum.
      - For EKS HPAs, it reads the current replicas (from hpa.status.currentReplicas) and compares it with the configured minimum (hpa.spec.minReplicas).
      
    If the actual running count (or replica count) is less than the configured minimum, that resource is marked as "FAILED".
    An overall status of "FAILED" is assigned if any resource has failed.
    
    Finally, the function publishes an SNS notification including:
      - The Lambda event input,
      - The CSV configuration row used,
      - And a detailed execution summary with individual statuses.
      
    SNS topic ARNs are provided via environment variables.
    """
    logger.info(f"Status check event input: {json.dumps(event, indent=2)}")
    csv_config = event.get("csv_config")
    if not csv_config:
        return {"status": "error", "message": "No CSV configuration provided in input"}
    
    status_report = {"ecs": {}, "asg": {}, "eks": {}}
    overall_status = "SUCCESS"
    
    # ----- ECS Status Check -----
    for key, value in csv_config.items():
        if key.startswith("ecs:") or key.startswith("ecs_"):
            # Extract region from key if present; otherwise use default aws_region.
            if key.startswith("ecs:"):
                remaining = key[len("ecs:"):]
                parts_region = remaining.split(":", 1)
                if len(parts_region) != 2:
                    continue
                resource_region = parts_region[0]
                remaining = parts_region[1]
            else:
                resource_region = aws_region
                remaining = key[len("ecs_"):]
            if '#' not in remaining:
                continue
            parts = remaining.split('#', 1)
            ecs_cluster = parts[0]
            remainder = parts[1]
            parts2 = remainder.rsplit('_', 1)
            if len(parts2) != 2:
                continue
            service_name = parts2[0]
            try:
                # Get the configured values from CSV (using either new or old keys)
                desired_config = int(csv_config.get(f"ecs:{resource_region}:{ecs_cluster}#{service_name}_desired", 
                                                    csv_config.get(f"ecs_{ecs_cluster}#{service_name}_desired")))
                min_config = int(csv_config.get(f"ecs:{resource_region}:{ecs_cluster}#{service_name}_min", 
                                                csv_config.get(f"ecs_{ecs_cluster}#{service_name}_min")))
                max_config = int(csv_config.get(f"ecs:{resource_region}:{ecs_cluster}#{service_name}_max", 
                                                csv_config.get(f"ecs_{ecs_cluster}#{service_name}_max")))
            except Exception:
                continue
            try:
                local_ecs_client = boto3.client('ecs', region_name=resource_region)
                response = local_ecs_client.describe_services(cluster=ecs_cluster, services=[service_name])
                if response.get("services"):
                    service = response["services"][0]
                    # Use the actual running count rather than desiredCount.
                    running_count = service.get("runningCount")
                    if running_count is None or running_count < min_config:
                        status = "FAILED"
                        overall_status = "FAILED"
                    else:
                        status = "OK"
                    status_report["ecs"][f"{ecs_cluster}#{service_name}"] = {
                        "configured": {"desired": desired_config, "min": min_config, "max": max_config},
                        "runningCount": running_count,
                        "status": status,
                        "region": resource_region
                    }
                else:
                    status_report["ecs"][f"{ecs_cluster}#{service_name}"] = {"error": "Service not found", "status": "FAILED", "region": resource_region}
                    overall_status = "FAILED"
            except Exception as e:
                logger.error(f"Error checking ECS service {service_name} in cluster {ecs_cluster} in region {resource_region}: {e}")
                status_report["ecs"][f"{ecs_cluster}#{service_name}"] = {"error": str(e), "status": "FAILED", "region": resource_region}
                overall_status = "FAILED"
    
    # ----- ASG Status Check -----
    for key, value in csv_config.items():
        if key.startswith("asg:") or key.startswith("asg_"):
            if key.startswith("asg:"):
                remaining = key[len("asg:"):]
                parts_region = remaining.split(":", 1)
                if len(parts_region) != 2:
                    continue
                resource_region = parts_region[0]
                remaining = parts_region[1]
            else:
                resource_region = aws_region
                remaining = key[len("asg_"):]
            parts = remaining.rsplit('_', 1)
            if len(parts) != 2:
                continue
            asg_name = parts[0]
            try:
                desired_config = int(csv_config.get(f"asg:{resource_region}:{asg_name}_desired", 
                                                    csv_config.get(f"asg_{asg_name}_desired")))
                min_config = int(csv_config.get(f"asg:{resource_region}:{asg_name}_min", 
                                                csv_config.get(f"asg_{asg_name}_min")))
                max_config = int(csv_config.get(f"asg:{resource_region}:{asg_name}_max", 
                                                csv_config.get(f"asg_{asg_name}_max")))
            except Exception:
                continue
            try:
                local_asg_client = boto3.client('autoscaling', region_name=resource_region)
                response = local_asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
                if response["AutoScalingGroups"]:
                    asg = response["AutoScalingGroups"][0]
                    # Count the number of instances in the ASG with LifecycleState "InService"
                    instances = asg.get("Instances", [])
                    in_service_count = sum(1 for instance in instances if instance.get("LifecycleState") == "InService")
                    if in_service_count < min_config:
                        status = "FAILED"
                        overall_status = "FAILED"
                    else:
                        status = "OK"
                    status_report["asg"][asg_name] = {
                        "configured": {"desired": desired_config, "min": min_config, "max": max_config},
                        "inServiceCount": in_service_count,
                        "status": status,
                        "region": resource_region
                    }
                else:
                    status_report["asg"][asg_name] = {"error": "ASG not found", "status": "FAILED", "region": resource_region}
                    overall_status = "FAILED"
            except Exception as e:
                logger.error(f"Error checking ASG {asg_name} in region {resource_region}: {e}")
                status_report["asg"][asg_name] = {"error": str(e), "status": "FAILED", "region": resource_region}
                overall_status = "FAILED"
    
    # ----- EKS HPA Status Check -----
    for key, value in csv_config.items():
        if key.startswith("eks:") or key.startswith("eks_"):
            if key.startswith("eks:"):
                remaining = key[len("eks:"):]
                parts = remaining.split(":", 2)
                if len(parts) != 3:
                    continue
                resource_region = parts[0]
                target_cluster = parts[1]
                remaining = parts[2]
                if '#' not in remaining:
                    continue
                parts = remaining.split("#", 1)
                eks_namespace = parts[0]
                remainder = parts[1]
                parts2 = remainder.rsplit('_', 1)
                if len(parts2) != 2:
                    continue
                hpa_name = parts2[0]
            else:
                remaining = key[len("eks_"):]
                resource_region = aws_region
                if ":" in remaining:
                    parts_cluster = remaining.split(":", 1)
                    target_cluster = parts_cluster[0]
                    remaining = parts_cluster[1]
                else:
                    target_cluster = "default-cluster"
                if '#' not in remaining:
                    continue
                parts = remaining.split('#', 1)
                eks_namespace = parts[0]
                remainder = parts[1]
                parts2 = remainder.rsplit('_', 1)
                if len(parts2) != 2:
                    continue
                hpa_name = parts2[0]
            try:
                eks_client_api = get_eks_client_for_hpa(target_cluster, target_region=resource_region)
                hpa_obj = eks_client_api.read_namespaced_horizontal_pod_autoscaler(name=hpa_name, namespace=eks_namespace)
                current_replicas = hpa_obj.status.currentReplicas if hasattr(hpa_obj.status, "currentReplicas") else None
                configured_min = hpa_obj.spec.minReplicas
                configured_max = hpa_obj.spec.maxReplicas
                # Here we only require that the running pods count is not less than the configured minimum.
                if current_replicas is None or current_replicas < configured_min:
                    hpa_status = "FAILED"
                    overall_status = "FAILED"
                else:
                    hpa_status = "OK"
                status_report["eks"][f"{target_cluster}:{eks_namespace}#{hpa_name}"] = {
                    "configured": {"min": configured_min, "max": configured_max},
                    "currentReplicas": current_replicas if current_replicas is not None else "N/A",
                    "status": hpa_status,
                    "region": resource_region
                }
            except ApiException as e:
                logger.error(f"Error reading HPA {hpa_name} in namespace {eks_namespace} of cluster {target_cluster}: {e}")
                status_report["eks"][f"{target_cluster}:{eks_namespace}#{hpa_name}"] = {"error": f"{e.status} - {e.reason}", "status": "FAILED", "region": resource_region}
                overall_status = "FAILED"
            except Exception as e:
                logger.error(f"Unexpected error reading HPA {hpa_name}: {e}")
                status_report["eks"][f"{target_cluster}:{eks_namespace}#{hpa_name}"] = {"error": str(e), "status": "FAILED", "region": resource_region}
                overall_status = "FAILED"
    
    # --- Step 10: Build overall status ---
    status_report["overall_status"] = overall_status
    logger.info("Status check report:")
    logger.info(json.dumps(status_report, indent=2))

    # --- Step 11: Publish SNS notification with status check report ---
    try:
        sns_client = boto3.client('sns', region_name=aws_region)
        consolidated_message = (
            f"Scaling status check completed for concurrency: {event.get('concurrency')}\n\n"
            f"Overall Execution Status: {overall_status}\n\n"
            f"Detailed Status Report:\n{json.dumps(status_report, indent=2)}"
        )
        sns_response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Scaling Status Check Report",
            Message=consolidated_message
        )
        logger.info(f"SNS status notification sent: {sns_response['MessageId']}")
        
        # If overall status FAILED, send an additional SNS message if failure topic is provided.
        if overall_status == "FAILED" and SNS_TOPIC_ARN_FAILURE:
            failure_message = (
                f"ALERT: Scaling status check FAILED for concurrency: {event.get('concurrency')}\n\n"
                f"Failure Report:\n{json.dumps(status_report, indent=2)}"
            )
            sns_failure_response = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN_FAILURE,
                Subject="Scaling Status Check FAILURE Notification",
                Message=failure_message
            )
            logger.info(f"SNS failure notification sent: {sns_failure_response['MessageId']}")
    except Exception as e:
        logger.error(f"Error publishing SNS status notification: {e}")

    return {
        "status": "success",
        "report": status_report
    }