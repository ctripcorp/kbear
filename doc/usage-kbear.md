# KBear Enterprise Kafka Solution

目录

- [Monitoring](#monitoring)
  - [Deploy Prometheus](#deploy-prometheus)
  - [Deploy Graphana](#deploy-graphana)
  - [Deploy Kafka Exporter](#deploy-kafka-exporter)
  - [Deploy Kafka JMX Exporter](#deploy-kafka-jmx-exporter)
  - [导入 Kafka Dashboard](#%E5%AF%BC%E5%85%A5-kafka-dashboard)
- [Multi-Cluster](#multi-cluster)
  - [CRRL (Cluster Route Rule Language)](#crrl-cluster-route-rule-language)
  - [KBear Meta Service](#kbear-meta-service)
  - [KBear Client](#kbear-client)

## Monitoring

### Deploy Prometheus

[配置参考](../tools/kafka/monitoring/prometheus/prometheus.yml)

[部署脚本参考](../tools/kafka/monitoring/prometheus/deploy-prometheus.sh)

多个集群可共用1个Prometheus。

### Deploy Graphana

[部署脚本参考](../tools/kafka/monitoring/grafana/deploy-grafana.sh)

多个Prometheus可共用1个Graphana。

### Deploy Kafka Exporter

[部署脚本参考](../tools/kafka/monitoring/kafka-exporter/deploy-kafka-exporter.sh)

1个Cluster，部署且只能部署在1台机器上。注意根据实际情况，修改 [start.sh](../tools/kafka/monitoring/kafka-exporter/start.sh) 里的kafka.server参数。

### Deploy Kafka JMX Exporter

[部署脚本参考](../tools/kafka/monitoring/kafka-jmx-exporter/deploy-kafka-jmx-exporter.sh)

1个Cluster，每个Broker都要部署。

### 导入 Kafka Dashboard

有5个dashboard，在目录 [kafka-dashboards](../tools/kafka/monitoring/grafana/kafka-dashboards/1.0) 下。

## Multi-Cluster

### CRRL (Cluster Route Rule Language)

CRRL 是专门为Kafka多集群设计的路由规则语言。

使用说明：[kbear-crrl](kbear-crrl.md)

### KBear Meta Service

项目：[kbear-meta](../java/meta-service)

使用说明：[kbear meta service](kbear-meta-service.md)

### KBear Client

项目：[kbear-client](../java/kbear-client)

使用说明：[kbear client](kbear-client.md)
