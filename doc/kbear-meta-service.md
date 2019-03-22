# KBear Meta Service

目录

- [数据类型](#%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B)
- [http协议头](#http%E5%8D%8F%E8%AE%AE%E5%A4%B4)
  - [method](#method)
  - [request](#request)
  - [response](#response)
- [路由服务](#%E8%B7%AF%E7%94%B1%E6%9C%8D%E5%8A%A1)
  - [producer](#producer)
  - [consumer](#consumer)

## 数据类型

```proto
message ResponseError {
    string code = 1;
    string message = 2;
}

message ResponseStatus {
    string ack = 1; // "success" | "partial_fail" | "fail"，partial_fail指在批量操作时部分成功部分失败
    ResponseError error = 2;
}

message Cluster {
    string id = 1;
    map<string, string> meta = 2;
}

message Topic {
    string id = 1;
    map<string, string> meta = 2;
}

message ConsumerGroupId {
    string groupName = 1;
    string topicId = 2;
}

message ConsumerGroup {
    ConsumerGroupId id = 1;
    map<string, string> meta = 2;
}

message Client {
    string id = 1;
    map<string, string> meta = 2;
}

message Route {
    string clusterId = 1;
    string topicId = 2;
}
```

## http协议头

### method

POST

### request

Content-Type: application/json

Accept: application/json

### response

Content-Type: application/json

## 路由服务

### producer

路径：/route/producer

契约

```proto
message FetchProducerRouteRequest {
    Client client = 1;
    repeated string topicIds = 2;
}

message FetchProducerRouteResponse {
    ResponseStatus status = 1;
    map<string, Route> topicIdRoutes = 2;
    map<string, Cluster> clusters = 3;
    map<string, Topic> topics = 4;
}
```

示例:

request

```json
{
    "client": {
        "id": "",
        "meta": {
            "appId": "",
            "idc": "",
            "env": "",
            "subEnv": "",
            "ip": "",
            "hostName": ""
        }
    },
    "topicIds": ["topicName1", "topicName2", ...]
}
```

response

```json
{
    "status": {
        "ack": "success",
        "error": {
            "code": "",
            "message": ""
        }
    },
    "topicIdRoutes": {
        "fx.kafka.demo.hello.run": {
            "clusterId": "fws",
            "topicId": "fx.kafka.demo.hello.run"
        }
    },
    "clusters": {
        "fws": {
            "id": "fws",
            "meta": {
                "zookeeper.connect": "10.2.7.137:2181,10.2.7.138:2181,10.2.7.139:2181",
                "bootstrap.servers": "10.2.74.6:9092,10.2.73.254:9092,10.2.73.255:9092"
            }
        }
    },
    "topics": {
        "fx.kafka.demo.hello.run": {
            "id": "fx.kafka.demo.hello.run",
            "meta": {
                "bu": "framework"
            }
        }
    }
}
```

### consumer

路径：/route/consumer

契约

```proto
message FetchConsumerRouteRequest {
    Client client = 1;
    repeated ConsumerGroupId consumerGroupIds = 2;
}

message ConsumerGroupIdRoutePair {
    ConsumerGroupId consumerGroupId = 1;
    Route route = 2;
}

message FetchConsumerRouteResponse {
    ResponseStatus status = 1;
    repeated ConsumerGroupIdRoutePair consumerGroupIdRoutes = 2;
    map<string, Cluster> clusters = 3;
    map<string, Topic> topics = 4;
    repeated ConsumerGroup consumerGroups = 5;
}
```

示例:

request

```json
{
    "client": {
        "id": "",
        "meta": {
            "appId": "",
            "idc": "",
            "env": "",
            "subEnv": "",
            "ip": "",
            "hostName": ""
        }
    },
    "consumerGroupIds": [
        {
            "groupName": "",
            "topicId": ""
        }
    ]
}
```

response

```json
{
    "status": {
        "ack": "success"
    },
    "consumerGroupIdRoutes": [
        {
            "consumerGroupId": {
                "groupName": "fx.hellobom.string.consumer",
                "topicId": "fx.kafka.demo.hello.run"
            },
            "route": {
                "clusterId": "uat",
                "topicId": "fx.hellobom.string"
            }
        }
    ],
    "clusters": {
        "uat": {
            "id": "uat",
            "meta": {
                "zookeeper.connect": "10.2.27.123:2181,10.2.27.124:2181,10.2.27.125:2181",
                "bootstrap.servers": "10.2.27.123:9092,10.2.27.124:9092,10.2.27.125:9092"
            }
        }
    },
    "topics": {
        "fx.hellobom.string": {
            "id": "fx.hellobom.string",
            "meta": {
                "bu": "basebiz"
            }
        }
    },
    "consumerGroups": [
        {
            "id": {
                "groupName": "fx.hellobom.string.consumer",
                "topicId": "fx.kafka.demo.hello.run"
            },
            "meta": {
                "bu": "basebiz"
            }
        }
    ]
}
```
