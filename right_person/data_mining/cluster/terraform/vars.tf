variable "cluster_state_bucket" {
  type = "string"
}

variable "cluster_region" {
  type = "string"
}

variable "cluster_id" {
  type = "string"
}

variable "vpc_id" {
  type = "string"
}

variable "subnet_id" {
  type = "string"
}

variable "ip_whitelist" {
  type = "list"
  default = []
}

variable "s3_bucket_access_whitelist" {
  type = "list"
}

variable "ssh_key_pair" {
  type = "string"
}

variable "extra_security_groups" {
  type = "list"
  default = []
}

variable "instance_ami" {
  type = "string"
}

variable "master_instance_type" {
  type = "string"
}

variable "slave_instance_type" {
  type = "string"
}

variable "instance_additional_disk_space_gb" {
  type = "string"
  default = "0"
}

variable "slave_count" {
  type = "string"
}
