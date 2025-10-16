name: Build, Push to ECR, Deploy to EC2 (via SSM)

on:
  push:
    branches: [ "main" ]
    paths-ignore: [ "**/*.md" ]
  workflow_dispatch:

jobs:
  build-push-deploy:
    runs-on: ubuntu-latest
    # Make sure this matches the environment name under "Settings → Environments"
    environment: opsnow-healthcheck
    permissions:
      id-token: write
      contents: read

    env:
      IMAGE_TAG: ${{ github.sha }}
      IMAGE_URI: ${{ vars.AWS_ACCOUNT_ID }}.dkr.ecr.${{ vars.AWS_REGION }}.amazonaws.com/${{ vars.ECR_REPOSITORY }}
      ENTRY_CMD: "python src/global_monitor.py"   # ✅ Updated to match your repo

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Configure AWS (assume GitHub OIDC role)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::621120073833:role/Opsnow
          aws-region: us-east-1
          audience: sts.amazonaws.com
          output-env-credentials: true

      - name: Login to ECR
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build & Push image
        uses: docker/build-push-action@v6
        with:
          context: .
          file: cicd/docker/Dockerfile
          push: true
          tags: |
            ${{ env.IMAGE_URI }}:${{ env.IMAGE_TAG }}
            ${{ env.IMAGE_URI }}:latest

      - name: Resolve pushed image digest
        id: digest
        run: |
          set -e
          DIGEST=$(aws ecr describe-images \
            --repository-name "${{ vars.ECR_REPOSITORY }}" \
            --image-ids imageTag="${{ env.IMAGE_TAG }}" \
            --query 'imageDetails[0].imageDigest' --output text)
          echo "IMAGE_DIGEST=$DIGEST" >> "$GITHUB_OUTPUT"
          echo "Resolved digest: $DIGEST"

      # --- Deploy via SSM to EC2 using Instance ID ---
      - name: Deploy to EC2 by Instance IDs
        if: ${{ vars.EC2_INSTANCE_IDS != '' }}
        run: |
          set -e
          aws ssm send-command \
            --region "${{ vars.AWS_REGION }}" \
            --document-name "AWS-RunShellScript" \
            --comment "Deploy ${{ env.IMAGE_URI }}@${{ steps.digest.outputs.IMAGE_DIGEST }}" \
            --parameters commands='[
              "set -e",
              "aws ecr get-login-password --region ${{ vars.AWS_REGION }} | docker login --username AWS --password-stdin ${{ vars.AWS_ACCOUNT_ID }}.dkr.ecr.${{ vars.AWS_REGION }}.amazonaws.com",
              "docker pull ${{ env.IMAGE_URI }}@${{ steps.digest.outputs.IMAGE_DIGEST }}",
              "docker stop opsnow-healthcheck || true",
              "docker rm opsnow-healthcheck || true",
              "docker run -d --name opsnow-healthcheck --restart unless-stopped ${{ env.IMAGE_URI }}@${{ steps.digest.outputs.IMAGE_DIGEST }} ${{ env.ENTRY_CMD }}"
            ]' \
            --instance-ids $(echo "${{ vars.EC2_INSTANCE_IDS }}" | tr -d ' ') \
            --output text

      # --- Optional fallback: deploy by EC2 tag ---
      - name: Deploy to EC2 by Tag
        if: ${{ vars.EC2_INSTANCE_IDS == '' }}
        run: |
          set -e
          IDS=$(aws ec2 describe-instances \
            --region "${{ vars.AWS_REGION }}" \
            --filters "Name=tag:${{ vars.EC2_TAG_KEY }},Values=${{ vars.EC2_TAG_VALUE }}" "Name=instance-state-name,Values=running" \
            --query "Reservations[].Instances[].InstanceId" --output text)
          if [ -z "$IDS" ]; then
            echo "No running instances found with tag ${{ vars.EC2_TAG_KEY }}=${{ vars.EC2_TAG_VALUE }}"
            exit 1
          fi
          aws ssm send-command \
            --region "${{ vars.AWS_REGION }}" \
            --document-name "AWS-RunShellScript" \
            --comment "Deploy ${{ env.IMAGE_URI }}@${{ steps.digest.outputs.IMAGE_DIGEST }}" \
            --parameters commands='[
              "set -e",
              "aws ecr get-login-password --region ${{ vars.AWS_REGION }} | docker login --username AWS --password-stdin ${{ vars.AWS_ACCOUNT_ID }}.dkr.ecr.${{ vars.AWS_REGION }}.amazonaws.com",
              "docker pull ${{ env.IMAGE_URI }}@${{ steps.digest.outputs.IMAGE_DIGEST }}",
              "docker stop opsnow-healthcheck || true",
              "docker rm opsnow-healthcheck || true",
              "docker run -d --name opsnow-healthcheck --restart unless-stopped ${{ env.IMAGE_URI }}@${{ steps.digest.outputs.IMAGE_DIGEST }} ${{ env.ENTRY_CMD }}"
            ]' \
            --instance-ids $IDS \
            --output text



