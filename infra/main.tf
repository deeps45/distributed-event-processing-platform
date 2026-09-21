# Deliberately deploys a SINGLE free-tier-eligible EC2 instance that
# self-hosts Kafka, Redis and Postgres via Docker Compose (the same
# docker-compose.yml used locally), rather than AWS's managed MSK /
# ElastiCache / RDS. This is a conscious cost-safety choice: MSK in
# particular is NOT covered by the free tier and typically costs $100+/month
# for even a minimal cluster. See ../README.md "AWS deployment" section
# before applying this.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_security_group" "app" {
  name        = "event-platform-sg"
  description = "Distributed event processing platform - demo instance"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
  }

  ingress {
    description = "API"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = [var.allowed_app_cidr]
  }

  ingress {
    description = "Prometheus"
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = [var.allowed_app_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project = "distributed-event-processing-platform"
  }
}

resource "aws_instance" "app" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.app.id]

  # Stays within the free tier's 30GB/month EBS allowance.
  root_block_device {
    volume_type = "gp3"
    volume_size = 20
  }

  user_data = templatefile("${path.module}/user_data.sh.tpl", {
    github_repo_url = var.github_repo_url
  })

  tags = {
    Name    = "event-processing-platform-demo"
    Project = "distributed-event-processing-platform"
  }
}
