resource "aws_db_instance" "benchmarks_metastore_service" {
  allocated_storage    = 50
  engine               = "mysql"
  engine_version       = "8.0.28"
  instance_class       = "db.m5.large"
  db_name              = "hive"
  username             = var.mysql_user
  password             = var.mysql_password
  skip_final_snapshot  = true
  availability_zone    = var.availability_zone1
  db_subnet_group_name = aws_db_subnet_group.benchmarks_metastore_service.name
  vpc_security_group_ids = [aws_security_group.allow_my_ip.id]
}

resource "aws_db_subnet_group" "benchmarks_metastore_service" {
  name       = "benchmarks_subnet_group_for_metastore_service"
  subnet_ids = [
    aws_subnet.benchmarks_subnet1.id,
    aws_subnet.benchmarks_subnet2.id
  ]
}
