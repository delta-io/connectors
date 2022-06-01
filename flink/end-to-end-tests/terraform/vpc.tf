resource "aws_vpc" "this" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_subnet" "benchmarks_subnet1" {
  vpc_id            = aws_vpc.this.id
  availability_zone = var.availability_zone1
  cidr_block        = "10.0.0.0/17"
}

# There are two subnets needed to create an RDS subnet group. In fact this one is unused.
# If DB subnet group is built using only one AZ, the following error is thrown:
#     The DB subnet group doesn't meet Availability Zone (AZ) coverage requirement.
#     Current AZ coverage: us-west-2a. Add subnets to cover at least 2 AZs.
resource "aws_subnet" "benchmarks_subnet2" {
  vpc_id            = aws_vpc.this.id
  availability_zone = var.availability_zone2
  cidr_block        = "10.0.128.0/17"
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
}
resource "aws_default_route_table" "public" {
  default_route_table_id = aws_vpc.this.default_route_table_id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
}

resource "aws_security_group" "allow_my_ip" {
  name        = "benchmarks_security_group"
  description = "Allows all inbound traffic from a specific IP."
  vpc_id      = aws_vpc.this.id
  ingress {
    description = "Allow inbound traffic from given IP."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["${var.user_ip_address}/32"]
  }
  egress {
    description      = "Allow all outbound traffic."
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}
