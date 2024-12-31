# provider "aws" {

# #     access_key = "${var.aws_access_key}"

# #     secret_key = "${var.aws_secret_key}"
#     region = "us-east-1"
# #     # version = "~> 4.0"
  
# }
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
    # access_key = "${var.aws_access_key}"
    # secret_key = "${var.aws_secret_key}"
    region = "us-east-1"
    shared_credentials_files = ["~/.aws/credentials"]
    profile = "default"
    
}

# # Create a VPC
# resource "aws_vpc" "example" {
#   cidr_block = "10.0.0.0/16"
# }