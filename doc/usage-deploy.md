# Kafka Cluster Deployment

目录

- [Machine Configuration](#machine-configuration)
- [Machine Init](#machine-init)
- [Deploy Kafka](#deploy-kafka)

## Machine Configuration

CPU：32Threads, @2.1GHz

MEM：64G

OS Disk：240G(SSD) * 2（冗余, 约240G可用）

数据盘：8TB(SATA) * 12，raid 10

网络接入：10Gb，冗余双网卡

可支撑数据量量：写入 150 M/s，读取 400 M/s

## Machine Init

[初始化脚本参考](../tools/kafka/init/init-machine.sh)

sudo权限执行，执行后reboot。注意具体机器配置可能不同，可能要修改脚本。

## Deploy Kafka

[部署脚本参考](../tools/kafka/deploy/deploy-kafka-service.sh)

注意根据具体机器修改：

- kafka-env.sh
- server.properties
