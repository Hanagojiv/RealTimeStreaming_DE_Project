# S3 Bucket for Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "${lower(var.project_name)}-data-lake"

  tags = {
    Name        = "${lower(var.project_name)}-data-lake"
    Environment = "Dev_env"
  }
}

# S3 Bucket Public Access Block
resource "aws_s3_bucket_public_access_block" "public_access" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket Policy for Redshift
resource "aws_s3_bucket_policy" "redshift_access_policy" {
  bucket = aws_s3_bucket.data_lake.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid       = "RedshiftS3Access",
        Effect    = "Allow",
        Principal = {
          Service = "redshift.amazonaws.com"
        },
        Action    = "s3:GetObject",
        Resource  = "${aws_s3_bucket.data_lake.arn}/*"
      }
    ]
  })
}

# IAM Role for Redshift
resource "aws_iam_role" "redshift_iam_role" {
  name = "${replace(lower(var.project_name), "_", "-")}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          Service = "redshift.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Attach AmazonS3ReadOnlyAccess to Redshift IAM Role
resource "aws_iam_policy_attachment" "redshift_s3_access" {
  name       = "RedshiftS3Access"
  roles      = [aws_iam_role.redshift_iam_role.name]
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# Redshift Cluster
resource "aws_redshift_cluster" "redshift_cluster" {
  cluster_identifier       = "redshift-cluster"
  database_name            = "user_data"
  master_username          = "admin_user_01"
  master_password          = var.redshift_master_password # Replace with a secure password
  node_type                = "dc2.large"
  cluster_type             = "single-node"
  publicly_accessible      = true
  iam_roles                = [aws_iam_role.redshift_iam_role.arn]
  skip_final_snapshot      = true

  tags = {
    Name = "RedshiftCluster"
  }
}
locals {
  redshift_hostname = regex("^([^:]+):5439$", aws_redshift_cluster.redshift_cluster.endpoint)[0]
}



# Create Redshift Schema
resource "null_resource" "create_redshift_schema" {
  provisioner "local-exec" {
  command = <<EOT
  PGPASSWORD=${var.redshift_master_password} psql \
  -h ${local.redshift_hostname} \
  -p 5439 \
  -U ${aws_redshift_cluster.redshift_cluster.master_username} \
  -d ${aws_redshift_cluster.redshift_cluster.database_name} \
  -v ON_ERROR_STOP=1 \
  -f /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/table_schema.sql
  EOT

  }

  depends_on = [aws_redshift_cluster.redshift_cluster]
}
# Create Redshift Schema
resource "null_resource" "update_redshift_schema" {
  provisioner "local-exec" {
  command = <<EOT
  PGPASSWORD=${var.redshift_master_password} psql \
  -h ${local.redshift_hostname} \
  -p 5439 \
  -U ${aws_redshift_cluster.redshift_cluster.master_username} \
  -d ${aws_redshift_cluster.redshift_cluster.database_name} \
  -v ON_ERROR_STOP=1 \
  -f /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/table_schema.sql
  EOT

  }

  depends_on = [aws_redshift_cluster.redshift_cluster]
}

resource "null_resource" "grant_permissions" {
  provisioner "local-exec" {
    command = <<EOT
    PGPASSWORD=${var.redshift_master_password} psql \
    -h ${local.redshift_hostname} \
    -p 5439 \
    -U ${aws_redshift_cluster.redshift_cluster.master_username} \
    -d ${aws_redshift_cluster.redshift_cluster.database_name} \
    -c "GRANT ALL PRIVILEGES ON SCHEMA public TO admin_user_01;"
    EOT
  }

  depends_on = [aws_redshift_cluster.redshift_cluster]
}



# Output for S3 Bucket and Redshift Cluster
output "s3_bucket_name" {
  value = aws_s3_bucket.data_lake.bucket
}

output "redshift_cluster_endpoint" {
  value = aws_redshift_cluster.redshift_cluster.endpoint
}

output "redshift_database_name" {
  value = aws_redshift_cluster.redshift_cluster.database_name
}
