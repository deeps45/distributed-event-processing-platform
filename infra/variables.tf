variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "Free-tier-eligible instance type. t3.micro/t2.micro give 750 hrs/month free for 12 months on a new account - do not change this without checking your AWS Billing > Free Tier usage page first."
  type        = string
  default     = "t3.micro"
}

variable "key_name" {
  description = "Name of an existing EC2 key pair (for SSH access). Create one in the AWS Console under EC2 > Key Pairs first."
  type        = string
}

variable "allowed_ssh_cidr" {
  description = "CIDR allowed to SSH in. Set this to YOUR_IP/32, never 0.0.0.0/0."
  type        = string
}

variable "allowed_app_cidr" {
  description = "CIDR allowed to reach the API (8000) and Prometheus (9090). Set this to YOUR_IP/32 for a personal demo."
  type        = string
}

variable "github_repo_url" {
  description = "Repo to clone and run on the instance"
  type        = string
  default     = "https://github.com/deeps45/distributed-event-processing-platform.git"
}
