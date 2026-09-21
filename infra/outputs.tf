output "instance_public_ip" {
  value = aws_instance.app.public_ip
}

output "api_url" {
  value = "http://${aws_instance.app.public_ip}:8000"
}

output "prometheus_url" {
  value = "http://${aws_instance.app.public_ip}:9090"
}

output "ssh_command" {
  value = "ssh ec2-user@${aws_instance.app.public_ip}"
}

output "next_steps" {
  value = "Instance takes ~2-3 min to finish 'docker-compose up' after launch. Check progress with: ssh ec2-user@<ip> 'sudo docker ps'. IMPORTANT: run `terraform destroy` when you're done to avoid ongoing charges."
}
