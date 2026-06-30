environment          = "prod"
aws_region           = "eu-west-1"
vpc_cidr             = "10.3.0.0/16"
kafka_version        = "3.5.1"
# m5.2xlarge for 2M+ records with head-room; scale horizontally via num_brokers
msk_broker_instance_type = "kafka.m5.2xlarge"
msk_num_brokers      = 3
kafka_topic          = "sales-events"
