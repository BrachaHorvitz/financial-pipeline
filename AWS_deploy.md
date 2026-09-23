# Deploying to AWS (ECR + ECS Fargate)

This guide walks through pushing the Docker image to Amazon ECR so it can be
run on ECS Fargate using `ecs-task-definition.json`.

---

## Step 1 — Install the AWS CLI

The AWS CLI lets you control AWS (create repositories, push images, deploy
services, etc.) from the terminal instead of the browser console.

macOS/Linux:

```bash
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
sudo installer -pkg AWSCLIV2.pkg -target /
```

Windows (PowerShell as administrator):

```powershell
winget install --id Amazon.AWSCLI --exact
```

Verify the install:

```bash
aws --version
# aws-cli/2.x.x ...
```

---

## Step 2 — Create an IAM user with deployment credentials

The CLI needs AWS credentials to act on your behalf. Access keys (an Access
Key ID + Secret Access Key pair) work like a username/password for the CLI.

In the AWS Console:

1. Go to **IAM → Users → Create user**.
2. Name it `finpipeline-deployer` (or similar).
3. Under permissions, choose **Attach policies directly** and add
   `AmazonEC2ContainerRegistryFullAccess` (lets you push/pull images to ECR).
4. Open the new user → **Security credentials** → **Create access key**.
5. Choose **CLI** as the use case, then copy the Access Key ID and Secret
   Access Key (the secret is shown only once).

---

## Step 3 — Configure the CLI

```bash
aws configure
```

You'll be prompted for four values:

```
AWS Access Key ID:     <your Access Key ID>
AWS Secret Access Key: <your Secret Access Key>
Default region name:   il-central-1   (or another region, e.g. eu-west-1)
Default output format: json
```

This stores your credentials locally in the AWS CLI's credentials file
(`~/.aws/credentials`), which every `aws` command reads.

Verify authentication:

```bash
aws sts get-caller-identity
```

You should see JSON with your account number and user ARN.

---

## Step 4 — Create an ECR repository

ECR (Elastic Container Registry) is AWS's private Docker registry — a place
to store images so ECS can pull them.

```bash
aws ecr create-repository \
  --repository-name finpipeline \
  --region il-central-1
```

The output includes a `repositoryUri`, e.g.:

```
<AWS_ACCOUNT_ID>.dkr.ecr.il-central-1.amazonaws.com/finpipeline
```

Replace `<AWS_ACCOUNT_ID>` with your own account number in every command
below.

---

## Step 5 — Authenticate Docker with ECR

ECR issues a temporary token that Docker uses to authenticate:

```bash
aws ecr get-login-password --region il-central-1 | \
  docker login --username AWS --password-stdin \
  <AWS_ACCOUNT_ID>.dkr.ecr.il-central-1.amazonaws.com
```

You should see `Login Succeeded`. The token is valid for 12 hours — re-run
this if you get an auth error later.

---

## Step 6 — Tag the image with the ECR address

The local image is named `financial-pipeline-app`; Docker needs it tagged
with the full ECR URI to know where to push it:

```bash
docker tag financial-pipeline-app:latest \
  <AWS_ACCOUNT_ID>.dkr.ecr.il-central-1.amazonaws.com/finpipeline:latest
```

This doesn't copy the image — it just adds an alias, the same way a file can
have two names.

---

## Step 7 — Push the image

```bash
docker push <AWS_ACCOUNT_ID>.dkr.ecr.il-central-1.amazonaws.com/finpipeline:latest
```

Docker uploads each layer; layers already present in ECR are skipped. Verify
the push:

```bash
aws ecr list-images --repository-name finpipeline --region il-central-1
```

---

## Summary

```
Local machine                        AWS ECR
financial-pipeline-app:latest  ──►   <AWS_ACCOUNT_ID>.dkr.ecr.il-central-1.amazonaws.com/finpipeline:latest
```

Once the image is in ECR, the next steps to run it are:

- **ECS (Fargate)** — run containers from this image, using
  `ecs-task-definition.json` as the task definition.
- **RDS** — a managed PostgreSQL database, replacing the local Postgres
  container.
- **Amazon MQ** — a managed RabbitMQ broker, replacing the local RabbitMQ
  container.
