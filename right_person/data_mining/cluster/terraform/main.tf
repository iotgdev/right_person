# TODO: get someone with terraform knowledge to separate this into modules

// SETUP
terraform {
  # can't use interpolation in this block, so data is ommited and provided later
  backend "s3" {}
}

provider "aws" {
  region = "${var.cluster_region}"
}

data "aws_vpc" "right_person_vpc" {
  id = "${var.vpc_id}"
}

data "aws_subnet" "right_person_subnet" {
  id = "${var.subnet_id}"
}

data "aws_security_group" "selected_additional_groups" {
  filter {
    name = "group-name"
    values = "${var.extra_security_groups}"
  }
}

data "aws_iam_policy_document" "s3_readwrite" {
  statement {
    actions = [
      "s3:Get*",
      "s3:Delete*",
      "s3:Head*",
      "s3:List*",
      "s3:Put*"
    ]
    resources = "${formatlist("arn:aws:s3:::%s*", var.s3_bucket_access_whitelist)}"
  }
}

resource "aws_iam_policy" "right_person_s3_readwrite_policy" {
    name = "right_person-${var.cluster_id}-assume_role-s3_readwrite_policy"
    policy = "${data.aws_iam_policy_document.s3_readwrite.json}"
}

data "aws_iam_policy_document" "generic_describe_permissions" {
  statement {
    actions = [
      "sts:AssumeRole",
      "ec2:Describe*",
      "cloudwatch:Describe*",
      "cloudwatch:List*",
      "cloudwatch:GetMetric*"
    ]
    resources = [
      "*"
    ]
  }
}

resource "aws_iam_policy" "right_person_generic_describe_policy" {
  name = "right_person-${var.cluster_id}-assume_role-generic_describe"
  policy = "${data.aws_iam_policy_document.generic_describe_permissions.json}"
}

data "aws_iam_policy_document" "assume-role" {
  statement {
    actions = [
      "sts:AssumeRole"
    ]
    principals {
      type = "Service"
      identifiers = [
        "ec2.amazonaws.com"
      ]
    }
  }
}

resource "aws_iam_role" "right_person_role" {
  name = "right_person-${var.cluster_id}-role"
  assume_role_policy = "${data.aws_iam_policy_document.assume-role.json}"
}


resource "aws_iam_instance_profile" "right_person_instance_profile" {
  name = "right_person-${var.cluster_id}-instance_profile"
  role = "${aws_iam_role.right_person_role.id}"
}

resource "aws_iam_policy_attachment" "right_person_s3_readwrite_policy_attachment" {
  name = "${aws_iam_role.right_person_role.name}"
  roles = ["${aws_iam_role.right_person_role.name}"]
  policy_arn = "${aws_iam_policy.right_person_s3_readwrite_policy.arn}"
}

resource "aws_iam_policy_attachment" "right_person_generic_describe_policy_attachment" {
  name = "${aws_iam_role.right_person_role.name}"
  roles = ["${aws_iam_role.right_person_role.name}"]
  policy_arn = "${aws_iam_policy.right_person_generic_describe_policy.arn}"
}

resource "aws_security_group" "right_person-spark-master" {
  name = "right_person-${var.cluster_id}-master"
  description = "Right Person Spark Nodes"
  vpc_id = "${var.vpc_id}"
  tags {
    Name = "right_person-${var.cluster_id}-instance"
    Role = "right_person-${var.cluster_id}-instance"
    Cluster = "${var.cluster_id}"
  }

  ingress {  # driver and whitelist can connect to web UI
    from_port = 8080
    protocol = "tcp"
    to_port = 8080
    cidr_blocks = "${formatlist("%s/32", var.ip_whitelist)}"
  }

  egress {  # web UI available to world
    from_port = 8080
    protocol = "tcp"
    to_port = 8080
    cidr_blocks = ["0.0.0.0/0"]
  }

}

resource "aws_security_group" "right_person-spark-node" {
  name = "right_person-${var.cluster_id}-node"
  description = "Right Person Spark Nodes"
  vpc_id = "${var.vpc_id}"
  tags {
    Name = "right_person-${var.cluster_id}-instance"
    Role = "right_person-${var.cluster_id}-instance"
    Cluster = "${var.cluster_id}"
  }

  ingress {  # driver and whitelist can connect to spark
    from_port = 7077
    protocol = "tcp"
    to_port = 7077
    cidr_blocks = "${formatlist("%s/32", var.ip_whitelist)}"
  }

  egress {  # spark available to driver and whitelist
    from_port = 7077
    protocol = "tcp"
    to_port = 7077
    cidr_blocks = "${formatlist("%s/32", var.ip_whitelist)}"
  }

  ingress {
    from_port = 7077
    protocol = "tcp"
    to_port = 7077
    self = true
  }

  egress {
    from_port = 7077
    protocol = "tcp"
    to_port = 7077
    self = true
  }

  ingress {  # for the block manager (intra-node connectivity)
    from_port = 50070
    protocol = "tcp"
    to_port = 50070
    self = true
  }

  egress {
    from_port = 50070
    protocol = "tcp"
    to_port = 50070
    self = true
  }

  ingress {
    from_port = 50070
    protocol = "tcp"
    to_port = 50070
    security_groups = ["${aws_security_group.right_person-spark-master.id}"]
  }

  egress {
    from_port = 50070
    protocol = "tcp"
    to_port = 50070
    security_groups = ["${aws_security_group.right_person-spark-master.id}"]
  }

  ingress {  # for the task scheduler (intra-node connectivity)
    from_port = 45523
    protocol = "tcp"
    to_port = 45523
    self = true
  }

  egress {
    from_port = 45523
    protocol = "tcp"
    to_port = 45523
    self = true
  }

  ingress {
    from_port = 45523
    protocol = "tcp"
    to_port = 45523
    security_groups = ["${aws_security_group.right_person-spark-master.id}"]
  }

  egress {
    from_port = 45523
    protocol = "tcp"
    to_port = 45523
    security_groups = ["${aws_security_group.right_person-spark-master.id}"]
  }



  ingress {
    from_port = 7077
    protocol = "tcp"
    to_port = 7077
    security_groups = ["${aws_security_group.right_person-spark-master.id}"]
  }

  egress {
    from_port = 7077
    protocol = "tcp"
    to_port = 7077
    security_groups = ["${aws_security_group.right_person-spark-master.id}"]
  }
}

data "template_file" "right_person-spark-master-user_data" {
  template = "${file("${path.module}/start_spark_master.sh")}"
}

data "template_file" "right_person-spark-slave-user_data" {
  template = "${file("${path.module}/start_spark_slave.sh")}"

  vars {
    master_address = "${aws_instance.right_person-spark-master.private_ip}"
    spark_max_mem_gb = "${var.slave_max_memory_gb}g"
  }
}

resource "aws_instance" "right_person-spark-master" {
  connection {
    user = "ubuntu"
  }
  ami = "${var.instance_ami}"
  instance_type = "${var.master_instance_type}"
  iam_instance_profile = "${aws_iam_instance_profile.right_person_instance_profile.name}"
  key_name = "${var.ssh_key_pair}"
  vpc_security_group_ids = ["${list(data.aws_security_group.selected_additional_groups.id, aws_security_group.right_person-spark-master.id, aws_security_group.right_person-spark-node.id)}"]
  subnet_id = "${var.subnet_id}"
  user_data = "${data.template_file.right_person-spark-master-user_data.rendered}"
  root_block_device {
    volume_size = "${12 + var.instance_additional_disk_space_gb}"
  }
  tags {
    Name = "right_person-${var.cluster_id}-master-${count.index + 1}"
    Role = "right_person-${var.cluster_id}-master-instance"
    Cluster = "right_person-${var.cluster_id}"
  }
  count = "1"
}

resource "aws_instance" "right_person-spark-slave" {
  connection {
    user = "ubuntu"
  }
  ami = "${var.instance_ami}"
  instance_type = "${var.slave_instance_type}"
  iam_instance_profile = "${aws_iam_instance_profile.right_person_instance_profile.name}"
  key_name = "${var.ssh_key_pair}"
  vpc_security_group_ids = ["${list(data.aws_security_group.selected_additional_groups.id, aws_security_group.right_person-spark-node.id)}"]
  subnet_id = "${var.subnet_id}"
  user_data = "${data.template_file.right_person-spark-slave-user_data.rendered}"
  root_block_device {
    volume_size = "${12 + var.instance_additional_disk_space_gb}"
  }
  tags {
    Name = "right_person-${var.cluster_id}-slave-${count.index + 1}"
    Role = "right_person-${var.cluster_id}-slave-instance"
    Cluster = "${var.cluster_id}"
  }
  count = "${var.slave_count}"
}
