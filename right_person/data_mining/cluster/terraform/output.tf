output "master_public_ipv4" {
  value = "${aws_instance.right_person-spark-master.public_ip}"
}