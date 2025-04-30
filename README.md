# MultiServiceScaler
A serverless Lambda utility to scale ECS, ASG, and EKS HPA resources across regions based on concurrency inputs. Reads config from S3 CSV and supports selective updates (min, max, desired) via JSON. Ideal for automated, multi-service scaling during high-traffic events
