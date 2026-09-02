  ---
Step 1 — Install the AWS CLI

The AWS CLI is a program you install once on your machine. It lets you control AWS (create buckets, push images, deploy servers, etc.) from the terminal
instead of clicking around in the browser.

Run this in your terminal (PowerShell as administrator works fine):

winget install --id Amazon.AWSCLI --exact

After it finishes, close and reopen your terminal so Windows picks up the new aws command, then verify:

aws --version
# should print: aws-cli/2.x.x ...

  ---
Step 2 — Create an AWS IAM user with credentials

The CLI needs to know who you are on AWS so it can act on your behalf. AWS uses access keys — a pair of strings (Access Key ID + Secret Access Key) that
work like a username and password for the CLI.

Do this in the AWS Console (browser):

1. Go to IAM → Users → Create user
2. Name it finpipeline-deployer (or anything you like)
3. On the permissions page, choose Attach policies directly and add:
   - AmazonEC2ContainerRegistryFullAccess — lets you push/pull images to ECR
4. After the user is created, go to that user → Security credentials → Create access key
5. Choose CLI as the use case → copy both the Access Key ID and Secret Access Key (you only see the secret once)

  ---
Step 3 — Configure the CLI with your credentials

Back in your terminal:

aws configure

It will ask four questions — answer like this:

AWS Access Key ID:     <paste your Access Key ID>
AWS Secret Access Key: <paste your Secret Access Key>
Default region name:   il-central-1        ← or whichever region you want (e.g. eu-west-1)
Default output format: json

This writes your credentials to C:\Users\bracha\.aws\credentials — a local file the CLI reads every time you run an aws command.

Verify it works:
aws sts get-caller-identity
You should see JSON with your account number and user name. If you see that, the CLI is authenticated.

  ---
Step 4 — Create an ECR repository

ECR (Elastic Container Registry) is AWS's private Docker Hub — a place to store your Docker images so AWS services (ECS, EKS, etc.) can pull them later.

aws ecr create-repository `
--repository-name finpipeline `
--region il-central-1

The output will include a line like:
"repositoryUri": "800075447618.dkr.ecr.il-central-1.amazonaws.com/finpipeline"

Copy that URI — you'll use it in the next two steps. Replace 800075447618 with your actual AWS account number.

  ---
Step 5 — Authenticate Docker with ECR

Docker needs a temporary password to push images to your private ECR repository. This command fetches that password from AWS and pipes it directly into
docker login:

aws ecr get-login-password --region il-central-1 | `
docker login --username AWS --password-stdin `
800075447618.dkr.ecr.il-central-1.amazonaws.com

You should see: Login Succeeded

Docker images need a tag that tells Docker where to push them. Right now your image is named financial-pipeline-app — Docker doesn't know that means ECR. You tag it with the full ECR URI:

docker tag financial-pipeline-app:latest `
800075447618.dkr.ecr.il-central-1.amazonaws.com/finpipeline:latest
800075447618.dkr.ecr.il-central-1.amazonaws.com


You should see: Login Succeeded

What's happening here: ECR gives tokens that expire after 12 hours. This command gets a fresh token and feeds it to Docker so Docker can authenticate for the next 12 hours. You run this
once per session (or re-run if you get an auth error later).

  ---

Step 6 — Tag the image with the ECR address
It will ask four questions — answer like this:

AWS Access Key ID:     <paste your Access Key ID>
AWS Secret Access Key: <paste your Secret Access Key>
Default region name:   il-central-1        ← or whichever region you want (e.g. eu-west-1)
Default output format: json

This writes your credentials to C:\Users\bracha\.aws\credentials — a local file the CLI reads every time you run an aws command.

Verify it works:
aws sts get-caller-identity
You should see JSON with your account number and user name. If you see that, the CLI is authenticated.
  ---
Step 4 — Create an ECR repository
ECR (Elastic Container Registry) is AWS's private Docker Hub — a place to store your Docker images so AWS services (ECS, EKS, etc.) can pull them later.

aws ecr create-repository `
--repository-name finpipeline `
--region il-central-1

The output will include a line like:
"repositoryUri": "800075447618.dkr.ecr.il-central-1.amazonaws.com/finpipeline"

Copy that URI — you'll use it in the next two steps. Replace 800075447618 with your actual AWS account number.

  ---
Step 5 — Authenticate Docker with ECR

Docker needs a temporary password to push images to your private ECR repository. This command fetches that password from AWS and pipes it directly into
docker login:

aws ecr get-login-password --region il-central-1 | `
docker login --username AWS --password-stdin `
800075447618.dkr.ecr.il-central-1.amazonaws.com

You should see: Login Succeeded

Docker images need a tag that tells Docker where to push them. Right now your image is named financial-pipeline-app — Docker doesn't know that means ECR. You tag it with the full ECR URI:

docker tag financial-pipeline-app:latest `
800075447618.dkr.ecr.il-central-1.800075447618.com/finpipeline:latest
800075447618.dkr.ecr.il-central-1.800075447618.com

You should see: Login Succeeded

What's happening here: ECR gives tokens that expire after 12 hours. This command gets a fresh token and feeds it to Docker so Docker can authenticate for the next 12 hours. You run this
once per session (or re-run if you get an auth error later).

  ---
Step 6 — Tag the image with the ECR address

Docker images need a tag that tells Docker where to push them. Right now your image is named financial-pipeline-app — Docker doesn't know that means ECR. You tag it with the full ECR    
URI:
docker tag financial-pipeline-app:latest `
800075447618.dkr.ecr.il-central-1.amazonaws.com/finpipeline:latest

docker tag financial-pipeline-app:latest `
800075447618.dkr.ecr.il-central-1.amazonaws.com/finpipeline:latest

This doesn't copy the image — it just adds an alias. Think of it like giving a file a second name.

  ---
Step 7 — Push the image

docker push 800075447618.dkr.ecr.il-central-1.amazonaws.com/finpipeline:latest

Docker uploads each layer of the image. You'll see progress bars per layer. Layers that already exist in ECR are skipped (only new/changed layers are uploaded — same as how docker pull
works).

When it finishes, verify it's there:
aws ecr list-images --repository-name finpipeline --region il-central-1

  ---
Summary of what you'll have

Your machine                     AWS ECR
─────────────────                ─────────────────────────────────────────────
financial-pipeline-app:latest ──► 800075447618.dkr.ecr.il-central-1.amazonaws.com/finpipeline:latest

Once the image is in ECR, the next steps (which we can do after this) are:
- ECS (Elastic Container Service) — tell AWS to run containers from this image
- RDS — a managed PostgreSQL database (replaces the Postgres container)
- Amazon MQ — managed RabbitMQ (replaces the RabbitMQ container)
