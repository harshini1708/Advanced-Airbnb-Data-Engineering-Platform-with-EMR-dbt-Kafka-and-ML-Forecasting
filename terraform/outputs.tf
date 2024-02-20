output "s3_bucket_name" {
  value = aws_s3_bucket.airbnb_data_lake.id
}

output "redshift_endpoint" {
  value = aws_redshift_cluster.airbnb_cluster.endpoint
}