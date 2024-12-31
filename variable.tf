variable "aws_region" {
    description = "AWS region to deploy resources"
    default = "us-east-1"
  
}


variable "project_name" {

    description = "kafka-streaming-data-engineering-project"
    default = "kafka-streaming-project"
  
}
# variable "redshift_master_password" {
#   description = "Master password for Redshift"
#   default     = "YourSecurePassword" # Replace with a secure password
# }
variable "redshift_master_password" {
  description = "Master password for the Redshift cluster"
  type        = string
  sensitive   = true
}
