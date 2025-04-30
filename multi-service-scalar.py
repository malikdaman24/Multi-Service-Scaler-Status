import boto3
import csv
import io
import logging
import base64
import os
import re
import json
from kubernetes import client
from kubernetes.client.rest import ApiException
from botocore.signers import RequestSigner

# -----------------------------------------------------------------------------
# Set up logging
# -----------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Global variables for default region and S3 config
# -----------------------------------------------------------------------------
aws_region = '<REGION>' # Default AWS region
S3_BUCKET = '<YOUR-BUCKET-NAME>'  # S3 bucket containing CSV config
CSV_KEY = '<CSV-FILE-NAME>'  # CSV file with scaling configuration

# Optionally, set your SNS topic ARN and Step Function ARN via environment variable
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
STEP_FUNCTION_ARN_STATUS = os.environ.get("STEP_FUNCTION_ARN_STATUS")  # For post-scaling status check

# -----------------------------------------------------------------------------
# AWS clients for ECS, ASG, and Application Auto Scaling (global defaults)
# -----------------------------------------------------------------------------
global_ecs_client = boto3.client('ecs', region_name=aws_region)
global_asg_client = boto3.client('autoscaling', region_name=aws_region)
global_appscaling_client = boto3.client('application-autoscaling', region_name=aws_region)

########################################################################
# Helper: Normalize canonical resource IDs for filtering
########################################################################
def normalize_resource_id(resource_id):
    """Return a lowercase, trimmed version of the resource ID for consistent filtering."""
    return resource_id.strip().lower()

########################################################################
# Helper: Safely convert string to integer, returns None if empty or non-numeric.
########################################################################
def safe_int(value):
    if value is None or value.strip() == "":
        return None
    try:
        return int(value.strip())
    except Exception:
        return None

########################################################################
# EKS Authentication and Client Setup for HPA updates (multi-region)
########################################################################
def get_bearer_token(target_cluster, region=None):
    """
    Generates an authentication token for a given EKS cluster using Botocore’s RequestSigner.
    
    Parameters:
      - target_cluster: Name of the target EKS cluster (must match aws-auth ConfigMap).
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
# Helper: Filter update dictionaries based on include/exclude lists
########################################################################
def filter_resources(updates, resource_type, include_set, exclude_set):
    """
    Filters the updates dictionary based on include and exclude sets.
    
    Parameters:
      - updates: The dictionary with keys as tuples (region, ...) for the resource.
      - resource_type: "ecs", "asg", or "eks"
      - include_set: A set of canonical resource IDs to include (if nonempty).
      - exclude_set: A set of canonical resource IDs to exclude.
      
    Returns a filtered dictionary containing only the resources that should be updated.
    """
    filtered = {}
    for key, params in updates.items():
        if resource_type == "ecs":
            cid = normalize_resource_id(f"ecs:{key[0]}:{key[1]}#{key[2]}")
        elif resource_type == "asg":
            cid = normalize_resource_id(f"asg:{key[0]}:{key[1]}")
        elif resource_type == "eks":
            cid = normalize_resource_id(f"eks:{key[0]}:{key[1]}:{key[2]}#{key[3]}")
        else:
            cid = ""
        if include_set and cid not in include_set:
            logger.info(f"Excluding resource {cid} as it is not in the include list.")
            continue
        if cid in exclude_set:
            logger.info(f"Excluding resource {cid} as it is in the exclude list.")
            continue
        filtered[key] = params
    return filtered

########################################################################
# Main Lambda Handler: Scaling Lambda
########################################################################
def lambda_handler(event, context):
    """
    Main Lambda handler that:
      1. Reads scaling configuration from a CSV file in S3.
      2. Selects the configuration row matching the provided "concurrency" value.
      3. Parses scaling parameters for ECS services, ASGs, and EKS HPAs.
      4. Applies include/exclude filtering so that if a resource is included, all its parameters (desired, min, max)
         are updated; if it is excluded, none are updated.
      5. Uses the "update_fields" input parameter (if provided) to determine which fields to update.
         For ECS/ASG, valid update fields are "desired", "min", and "max".
         For EKS HPAs, only "min" and "max" are applicable.
         For any field not specified in "update_fields", the current value is retrieved so that it remains unchanged.
      6. Updates ECS services, ASGs, and EKS HPAs accordingly.
      7. Publishes an SNS notification email with:
           - The Lambda event input,
           - The CSV configuration row used,
           - And the execution summary.
      8. Triggers a Step Function for post-scaling status check if STEP_FUNCTION_ARN_STATUS is provided.
         (This Step Function will wait 30 minutes and then trigger a separate status-check Lambda.)
      9. Returns a summary of successes and failures.
    
    The Lambda event may include:
      - "concurrency": The concurrency value to select the CSV configuration row.
      - "update_fields": A comma-separated list of fields to update. For ECS/ASG: "desired", "min", "max"; for EKS HPAs: "min", "max".
         If a field is not listed, then that field’s current value is left unchanged.
      - "include_resources": (Optional) Comma-separated list of canonical resource IDs to include.
      - "exclude_resources": (Optional) Comma-separated list of canonical resource IDs to exclude.
    
    Canonical resource ID formats:
      - ECS:  ecs:<region>:<cluster>#<service>
      - ASG:  asg:<region>:<asg>
      - EKS HPA: eks:<region>:<cluster>:<namespace>#<hpa>
    
    If "include_resources" is omitted or empty, all resources are processed (subject to exclusion).
    """
    # --- Step 1: Parse "concurrency" from event ---
    try:
        concurrency = float(event.get('concurrency'))
        logger.info(f"Received concurrency: {concurrency}")
    except Exception as e:
        logger.error(f"Invalid concurrency input: {event}. Error: {e}")
        return {"status": "error", "message": "Invalid concurrency input"}

    # --- Step 2: Parse "update_fields" from event ---
    # If not provided, update all fields.
    update_fields_str = event.get('update_fields', "desired,min,max")
    update_fields = {field.strip().lower() for field in update_fields_str.split(',')}
    logger.info(f"Fields to update: {update_fields}")

    # --- Step 3: Retrieve CSV content from S3 ---
    s3 = boto3.client('s3')
    try:
        csv_obj = s3.get_object(Bucket=S3_BUCKET, Key=CSV_KEY)
        csv_content = csv_obj['Body'].read().decode('utf-8-sig')
    except Exception as e:
        logger.error(f"Error reading CSV from S3: {e}")
        return {"status": "error", "message": "Error reading CSV from S3"}
    logger.info("CSV Content:\n" + csv_content)

    # --- Step 4: Select configuration row matching "concurrency" ---
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    config_row = None
    for row in csv_reader:
        try:
            raw_value = row['concurrency'].strip()
            if float(raw_value) == concurrency:
                config_row = row
                break
        except Exception as e:
            logger.warning(f"Skipping row due to error: {e}")
            continue
    if not config_row:
        message = f"No configuration found for concurrency: {concurrency}"
        logger.error(message)
        return {"status": "error", "message": message}
    logger.info(f"Configuration found: {config_row}")

    # --- Step 5: Process include/exclude filters from event ---
    include_set = set()
    exclude_set = set()
    if 'include_resources' in event and event['include_resources']:
        include_set = {normalize_resource_id(r) for r in event['include_resources'].split(',')}
        logger.info(f"Resources to include: {include_set}")
    if 'exclude_resources' in event and event['exclude_resources']:
        exclude_set = {normalize_resource_id(r) for r in event['exclude_resources'].split(',')}
        logger.info(f"Resources to exclude: {exclude_set}")

    # --- Step 6: Parse scaling parameters from CSV row ---
    # For all keys, use safe_int() to avoid conversion errors
    ecs_updates = {}   # Key: (region, cluster, service) -> dict with keys 'desired', 'min', 'max'
    asg_updates = {}   # Key: (region, asg) -> dict with keys 'desired', 'min', 'max'
    eks_updates = {}   # Key: (region, cluster, namespace, hpa) -> dict with keys 'min', 'max'
    
    for key, value in config_row.items():
        if key == "concurrency":
            continue
        
        # --- Process ECS keys ---
        if key.startswith("ecs:"):
            remaining = key[len("ecs:"):]
            parts_region = remaining.split(":", 1)
            if len(parts_region) != 2:
                logger.warning(f"Malformed ECS key with region specification: {key}")
                continue
            resource_region = parts_region[0]
            remaining = parts_region[1]
            if '#' not in remaining:
                logger.warning(f"Skipping malformed ECS key (missing '#'): {key}")
                continue
            parts = remaining.split('#', 1)
            ecs_cluster = parts[0]
            remainder = parts[1]
            parts2 = remainder.rsplit('_', 1)
            if len(parts2) != 2:
                logger.warning(f"Skipping malformed ECS key (cannot split service and suffix): {key}")
                continue
            service_name = parts2[0]
            suffix = parts2[1]
            ecs_updates.setdefault((resource_region, ecs_cluster, service_name), {})[suffix] = safe_int(value)
        elif key.startswith("ecs_"):
            remaining = key[len("ecs_"):]
            resource_region = aws_region
            if '#' not in remaining:
                logger.warning(f"Skipping malformed ECS key (missing '#'): {key}")
                continue
            parts = remaining.split('#', 1)
            ecs_cluster = parts[0]
            remainder = parts[1]
            parts2 = remainder.rsplit('_', 1)
            if len(parts2) != 2:
                logger.warning(f"Skipping malformed ECS key (cannot split service and suffix): {key}")
                continue
            service_name = parts2[0]
            suffix = parts2[1]
            ecs_updates.setdefault((resource_region, ecs_cluster, service_name), {})[suffix] = safe_int(value)

        # --- Process ASG keys ---
        elif key.startswith("asg:"):
            remaining = key[len("asg:"):]
            parts_region = remaining.split(":", 1)
            if len(parts_region) != 2:
                logger.warning(f"Malformed ASG key with region specification: {key}")
                continue
            resource_region = parts_region[0]
            remaining = parts_region[1]
            parts = remaining.rsplit('_', 1)
            if len(parts) != 2:
                logger.warning(f"Skipping malformed ASG key: {key}")
                continue
            asg_name = parts[0]
            suffix = parts[1]
            asg_updates.setdefault((resource_region, asg_name), {})[suffix] = safe_int(value)
        elif key.startswith("asg_"):
            remaining = key[len("asg_"):]
            resource_region = aws_region
            parts = remaining.rsplit('_', 1)
            if len(parts) != 2:
                logger.warning(f"Skipping malformed ASG key: {key}")
                continue
            asg_name = parts[0]
            suffix = parts[1]
            asg_updates.setdefault((resource_region, asg_name), {})[suffix] = safe_int(value)

        # --- Process EKS HPA keys ---
        elif key.startswith("eks:"):
            remaining = key[len("eks:"):]
            parts = remaining.split(":", 2)
            if len(parts) != 3:
                logger.warning(f"Malformed EKS key with region: {key}")
                continue
            resource_region = parts[0]
            target_cluster = parts[1]
            remaining = parts[2]
            if '#' not in remaining:
                logger.warning(f"Skipping malformed EKS key (missing '#'): {key}")
                continue
            parts = remaining.split("#", 1)
            eks_namespace = parts[0]
            remainder = parts[1]
            parts2 = remainder.rsplit('_', 1)
            if len(parts2) != 2:
                logger.warning(f"Skipping malformed EKS key (cannot split HPA and suffix): {key}")
                continue
            hpa_name = parts2[0]
            suffix = parts2[1]
            # Only "min" and "max" are applicable for EKS
            if suffix not in ['min', 'max']:
                logger.warning(f"Skipping unsupported EKS suffix '{suffix}' in key: {key}")
                continue
            eks_updates.setdefault((resource_region, target_cluster, eks_namespace, hpa_name), {})[suffix] = safe_int(value)
        elif key.startswith("eks_"):
            remaining = key[len("eks_"):]
            resource_region = aws_region
            if ":" in remaining:
                parts_cluster = remaining.split(":", 1)
                target_cluster = parts_cluster[0]
                remaining = parts_cluster[1]
            else:
                target_cluster = "default-cluster"
            if '#' not in remaining:
                logger.warning(f"Skipping malformed EKS key (missing '#'): {key}")
                continue
            parts = remaining.split('#', 1)
            eks_namespace = parts[0]
            remainder = parts[1]
            parts2 = remainder.rsplit('_', 1)
            if len(parts2) != 2:
                logger.warning(f"Skipping malformed EKS key (cannot split HPA and suffix): {key}")
                continue
            hpa_name = parts2[0]
            suffix = parts2[1]
            if suffix not in ['min', 'max']:
                logger.warning(f"Skipping unsupported EKS suffix '{suffix}' in key: {key}")
                continue
            eks_updates.setdefault((resource_region, target_cluster, eks_namespace, hpa_name), {})[suffix] = safe_int(value)

    # --- Step 6b: Apply filtering to updates ---
    ecs_updates = filter_resources(ecs_updates, "ecs", include_set, exclude_set)
    asg_updates = filter_resources(asg_updates, "asg", include_set, exclude_set)
    eks_updates = filter_resources(eks_updates, "eks", include_set, exclude_set)

    # --- Step 7: Update ECS Services ---
    ecs_success = []
    ecs_failure = []
    for (region, ecs_cluster, service_name), params in ecs_updates.items():
        # Retrieve current ECS service values if not provided in update_fields
        ecs_client_local = boto3.client('ecs', region_name=region)
        try:
            service_desc = ecs_client_local.describe_services(cluster=ecs_cluster, services=[service_name])
            if service_desc.get("services"):
                current_service = service_desc["services"][0]
            else:
                raise Exception("Service not found")
        except Exception as e:
            logger.error(f"Error describing ECS service {service_name} in cluster {ecs_cluster}: {e}")
            ecs_failure.append({"cluster": ecs_cluster, "service": service_name, "region": region, "error": str(e)})
            continue

        # For each field, if it is in update_fields and a CSV value is provided (i.e. not None), then use it; otherwise fetch the current value.
        if "desired" in update_fields:
            desired_count = params.get("desired") if params.get("desired") is not None else current_service.get("desiredCount")
        else:
            desired_count = current_service.get("desiredCount")

        try:
            appscaling_client_local = boto3.client('application-autoscaling', region_name=region)
            st_response = appscaling_client_local.describe_scalable_targets(
                ServiceNamespace='ecs',
                ResourceIds=[f"service/{ecs_cluster}/{service_name}"],
                ScalableDimension='ecs:service:DesiredCount'
            )
            if st_response["ScalableTargets"]:
                current_st = st_response["ScalableTargets"][0]
            else:
                raise Exception("Scalable target not found")
        except Exception as e:
            logger.warning(f"Error retrieving scalable target for ECS service {service_name} in cluster {ecs_cluster}: {e}")
            current_st = {}

        if "min" in update_fields:
            min_capacity = params.get("min") if params.get("min") is not None else current_st.get("MinCapacity")
        else:
            min_capacity = current_st.get("MinCapacity", current_service.get("desiredCount"))
        if "max" in update_fields:
            max_capacity = params.get("max") if params.get("max") is not None else current_st.get("MaxCapacity")
        else:
            max_capacity = current_st.get("MaxCapacity", current_service.get("desiredCount"))

        logger.info(f"Updating ECS service: Region='{region}', Cluster='{ecs_cluster}', Service='{service_name}', Desired={desired_count}, Min={min_capacity}, Max={max_capacity}")

        update_success = True
        update_errors = []
        try:
            ecs_client_local.update_service(
                cluster=ecs_cluster,
                service=service_name,
                desiredCount=desired_count
            )
            logger.info(f"ECS service '{service_name}' in cluster '{ecs_cluster}' updated to desired count {desired_count}.")
        except Exception as e:
            error_msg = f"update_service error: {e}"
            logger.error(f"ECS service '{service_name}' in cluster '{ecs_cluster}' failed: {error_msg}")
            update_success = False
            update_errors.append(error_msg)
        try:
            resource_id = f"service/{ecs_cluster}/{service_name}"
            appscaling_client_local.register_scalable_target(
                ServiceNamespace='ecs',
                ResourceId=resource_id,
                ScalableDimension='ecs:service:DesiredCount',
                MinCapacity=min_capacity,
                MaxCapacity=max_capacity
            )
            logger.info(f"Scalable target for ECS service '{service_name}' in cluster '{ecs_cluster}' updated with Min={min_capacity} and Max={max_capacity}.")
        except Exception as e:
            error_msg = f"register_scalable_target error: {e}"
            logger.error(f"Scalable target for ECS service '{service_name}' in cluster '{ecs_cluster}' failed: {error_msg}")
            update_success = False
            update_errors.append(error_msg)
        
        if update_success:
            ecs_success.append({"cluster": ecs_cluster, "service": service_name, "region": region})
        else:
            ecs_failure.append({"cluster": ecs_cluster, "service": service_name, "region": region, "errors": update_errors})

    # --- Step 8: Update ASGs ---
    asg_success = []
    asg_failure = []
    for (region, asg_name), params in asg_updates.items():
        asg_client_local = boto3.client('autoscaling', region_name=region)
        try:
            response = asg_client_local.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
            if response["AutoScalingGroups"]:
                current_asg = response["AutoScalingGroups"][0]
            else:
                raise Exception("ASG not found")
        except Exception as e:
            logger.error(f"Error describing ASG {asg_name} in region {region}: {e}")
            asg_failure.append({"asg": asg_name, "region": region, "error": str(e)})
            continue

        if "desired" in update_fields:
            desired_capacity = params.get("desired") if params.get("desired") is not None else current_asg.get("DesiredCapacity")
        else:
            desired_capacity = current_asg.get("DesiredCapacity")
        if "min" in update_fields:
            min_size = params.get("min") if params.get("min") is not None else current_asg.get("MinSize")
        else:
            min_size = current_asg.get("MinSize")
        if "max" in update_fields:
            max_size = params.get("max") if params.get("max") is not None else current_asg.get("MaxSize")
        else:
            max_size = current_asg.get("MaxSize")

        logger.info(f"Updating ASG: Region='{region}', ASG='{asg_name}', Desired={desired_capacity}, Min={min_size}, Max={max_size}")

        try:
            asg_client_local.update_auto_scaling_group(
                AutoScalingGroupName=asg_name,
                DesiredCapacity=desired_capacity,
                MinSize=min_size,
                MaxSize=max_size
            )
            logger.info(f"ASG '{asg_name}' updated successfully in region '{region}'.")
            asg_success.append({"asg": asg_name, "region": region})
        except Exception as e:
            error_msg = f"update_auto_scaling_group error: {e}"
            logger.error(f"ASG '{asg_name}' update failed: {error_msg}")
            asg_failure.append({"asg": asg_name, "region": region, "errors": [error_msg]})

    # --- Step 9: Update EKS HPAs ---
    eks_success = []
    eks_failure = []
    if eks_updates:
        for (region, target_cluster, eks_namespace, hpa_name), params in eks_updates.items():
            try:
                eks_client_api = get_eks_client_for_hpa(target_cluster, target_region=region)
                current_hpa = eks_client_api.read_namespaced_horizontal_pod_autoscaler(name=hpa_name, namespace=eks_namespace)
            except Exception as e:
                logger.error(f"Error reading HPA {hpa_name} in namespace {eks_namespace} of cluster {target_cluster}: {e}")
                eks_failure.append({"cluster": target_cluster, "namespace": eks_namespace, "hpa": hpa_name, "region": region, "error": str(e)})
                continue

            # For EKS HPAs, only "min" and "max" are applicable.
            if "min" in update_fields:
                new_min = params.get("min") if params.get("min") is not None else getattr(current_hpa.spec, "min_replicas", None)
            else:
                new_min = getattr(current_hpa.spec, "min_replicas", None)
            if "max" in update_fields:
                new_max = params.get("max") if params.get("max") is not None else getattr(current_hpa.spec, "max_replicas", None)
            else:
                new_max = getattr(current_hpa.spec, "max_replicas", None)

            logger.info(f"Updating EKS HPA: Region='{region}', Cluster='{target_cluster}', Namespace='{eks_namespace}', HPA='{hpa_name}', New min={new_min}, New max={new_max}")
            patch_body = {
                "spec": {
                    "minReplicas": new_min,
                    "maxReplicas": new_max
                }
            }
            try:
                eks_api = get_eks_client_for_hpa(target_cluster, target_region=region)
                eks_api.patch_namespaced_horizontal_pod_autoscaler(
                    name=hpa_name,
                    namespace=eks_namespace,
                    body=patch_body
                )
                logger.info(f"EKS HPA '{hpa_name}' in namespace '{eks_namespace}' (cluster: {target_cluster}, region: {region}) updated successfully.")
                eks_success.append({"cluster": target_cluster, "namespace": eks_namespace, "hpa": hpa_name, "region": region})
            except ApiException as e:
                error_msg = f"Kubernetes API error: {e.status} - {e.reason}"
                logger.error(f"EKS HPA '{hpa_name}' update failed: {error_msg}\nPatch body: {patch_body}")
                eks_failure.append({"cluster": target_cluster, "namespace": eks_namespace, "hpa": hpa_name, "region": region, "errors": [error_msg]})
            except Exception as e:
                error_msg = f"patch_namespaced_horizontal_pod_autoscaler error: {e}"
                logger.error(f"EKS HPA '{hpa_name}' update failed: {error_msg}\nPatch body: {patch_body}")
                eks_failure.append({"cluster": target_cluster, "namespace": eks_namespace, "hpa": hpa_name, "region": region, "errors": [error_msg]})
    
    # --- Step 10: Build execution summary ---
    summary = {
        "ecs": {"success": ecs_success, "failed": ecs_failure},
        "asg": {"success": asg_success, "failed": asg_failure},
        "eks": {"success": eks_success, "failed": eks_failure}
    }
    
    logger.info("Execution Summary:")
    logger.info(json.dumps(summary, indent=2))

    # --- Step 11: Publish SNS notification including Lambda input, CSV row, and summary ---
    try:
        sns_client = boto3.client('sns', region_name=aws_region)
        csv_output = json.dumps(config_row, indent=2)
        event_output = json.dumps(event, indent=2)
        consolidated_message = (
            f"Scaling updates applied for concurrency: {concurrency}\n\n"
            f"Lambda event input:\n{event_output}\n\n"
            f"Execution Summary:\n{json.dumps(summary, indent=2)}\n\n"
            f"CSV configuration row used:\n{csv_output}"
        )
        sns_response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Scaling Updates Summary Notification",
            Message=consolidated_message
        )
        logger.info(f"SNS notification sent: {sns_response['MessageId']}")
    except Exception as e:
        logger.error(f"Error publishing SNS notification: {e}")

    # --- Step 12: Trigger Step Function for Post-Scaling Status Check ---
    try:
        step_function_client = boto3.client('stepfunctions', region_name=aws_region)
        if STEP_FUNCTION_ARN_STATUS:
            status_sf_response = step_function_client.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN_STATUS,
                input=json.dumps({
                    "concurrency": concurrency,
                    "csv_config": config_row,
                    "scaling_summary": summary,
                    "original_event": event
                })
            )
            logger.info(f"Post-scaling status check Step Function triggered: {status_sf_response['executionArn']}")
    except Exception as e:
        logger.error(f"Error triggering post-scaling status check step function: {e}")

    return {
        "status": "success",
        "message": f"Scaling updates applied for concurrency: {concurrency}",
        "summary": summary
    }