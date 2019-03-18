#!/usr/bin/env python
# -*- coding: utf-8 -*-


_ASSUME_ROLE = {
    "Version": "2012-10-17",
    "Statement": {
        "Effect": "Allow",
        "Principal": {"Service": "ec2.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }
}


_S3_READ_WRITE = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Action": [
                "s3:Put*",
                "s3:List*",
                "s3:Head*",
                "s3:Get*",
                "s3:Delete*"
            ],
            "Resource": "*"
        }
    ]
}


_GENERIC_DESCRIBE = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole",
                "ec2:Describe*",
                "cloudwatch:List*",
                "cloudwatch:GetMetric*",
                "cloudwatch:Describe*"
            ],
            "Resource": "*"
        }
    ]
}
