environment          = "dev"
aws_region           = "eu-west-1"
vpc_cidr             = "10.0.0.0/16"
kafka_version        = "3.5.1"
# Smallest broker for dev — cheapest, single-AZ friendly
msk_broker_instance_type = "kafka.t3.small"
msk_num_brokers      = 1
kafka_topic          = "sales-events"
