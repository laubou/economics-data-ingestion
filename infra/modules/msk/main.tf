locals {
  name = "${var.project_name}-${var.environment}"
}

resource "aws_msk_configuration" "main" {
  name              = "${local.name}-config"
  kafka_versions    = [var.kafka_version]
  server_properties = <<-EOT
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=3
    log.retention.hours=168
    log.segment.bytes=1073741824
    # Enables log compaction — useful if we add a compacted changelog topic later
    log.cleanup.policy=delete
  EOT
}

resource "aws_msk_cluster" "main" {
  cluster_name           = local.name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.num_brokers

  broker_node_group_info {
    instance_type  = var.broker_instance_type
    client_subnets = var.subnet_ids
    storage_info {
      ebs_storage_info {
        volume_size = 100
      }
    }
    security_groups = [var.security_group_id]
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  # Enable MSK Connect for future Kafka Connect sink connectors (e.g. S3 sink)
  open_monitoring {
    prometheus {
      jmx_exporter  { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = { Name = local.name }
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${local.name}"
  retention_in_days = 14
}

# ---- Topic (via null_resource + aws CLI, MSK doesn't have a native TF resource) ----
# In a real deployment, topics are managed via a Kafka admin client or Confluent provider.
# We document the desired config here as a local value for reference.
locals {
  topic_config = {
    name               = var.kafka_topic
    partitions         = 3
    replication_factor = 3
    # Keyed by order_id → same order always lands on same partition
    # Retention: 7 days (data is durable in S3 bronze after consumption)
    retention_ms = 604800000
  }
}
