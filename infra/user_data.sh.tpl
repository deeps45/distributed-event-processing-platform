#!/bin/bash
set -euxo pipefail

dnf update -y
dnf install -y docker git

systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

DOCKER_COMPOSE_VERSION="v2.29.7"
curl -SL "https://github.com/docker/compose/releases/download/$${DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64" \
  -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

cd /home/ec2-user
git clone ${github_repo_url} app
cd app

/usr/local/bin/docker-compose up -d --build
