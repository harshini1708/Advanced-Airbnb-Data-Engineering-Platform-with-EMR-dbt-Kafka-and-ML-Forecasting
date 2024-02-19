provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "airbnb_data_lake" {
  bucket = var.s3_bucket_name
  acl    = "private"
}

resource "aws_redshift_cluster" "airbnb_cluster" {
  cluster_identifier = var.redshift_cluster_id
  node_type          = "dc2.large"
  number_of_nodes    = 2
  database_name      = "airbnb"
  master_username    = var.redshift_username
  master_password    = var.redshift_password
  skip_final_snapshot = true
}

resource "aws_emr_cluster" "airbnb_emr" {
  name          = "AirbnbETLCluster"
  release_label = "emr-6.3.0"
  applications  = ["Hadoop", "Spark"]
  log_uri       = "s3://${aws_s3_bucket.airbnb_data_lake.bucket}/logs/"
  service_role  = aws_iam_role.emr_service.arn
  ec2_attributes {
    instance_profile = aws_iam_instance_profile.emr_profile.arn
  }
  master_instance_type = "m5.xlarge"
  core_instance_type   = "m5.xlarge"
  core_instance_count  = 2
}