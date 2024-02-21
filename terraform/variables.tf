variable "region" {
  default = "us-west-2"
}

variable "s3_bucket_name" {
  default = "airbnb-etl-bucket"
}

variable "redshift_cluster_id" {
  default = "airbnb-cluster"
}

variable "redshift_username" {
  default = "awsuser"
}

variable "redshift_password" {
  default = "ReplaceWithSecurePassword"
}