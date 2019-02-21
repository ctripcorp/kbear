package com.ctrip.framework.kbear.client;

/**
 * @author koqizhao
 *
 * Jan 25, 2019
 */
public interface TestData {

    /* for basic test start */

    String META_SERVICE_URL = "http://meta.kafka.fx.fws.qa.nt.ctripcorp.com";

    String META_SERVICE_PROPERTY_KEY = "kafka.meta.service.url";

    // in cluster 1
    String TOPIC = "fx.kafka.demo.hello.run";

    // in cluster 2
    String TOPIC_2 = "fx.kafka.demo.hello.run2";

    // in cluster 1
    String TOPIC_3 = "fx.kafka.demo.hello.run3";

    String CONSUMER_GROUP = "fx.kafka.demo.hello.consumer";
    String CONSUMER_GROUP_2 = "fx.kafka.demo.hello.consumer2";

    /* for basic test end */

    /* for restart test start */

    String META_SERVICE_URL_2 = "http://meta.kafka.fx.uat.qa.nt.ctripcorp.com";

    // for url 1, topic 4 in cluster 1
    // for url 2, topic 4 in cluster 2
    // no matter what url, (consumer group, topic 4) in cluster 1
    // no matter what url, (consumer group 2, topic 4) in cluster 2
    String TOPIC_4 = "fx.kafka.demo.hello.run4";

    // no matter what url, topic 5 in cluster 1
    // for url 1, (consumer group, topic 5) in cluster 1
    // for url 2, (consumer group, topic 5) in cluster 2
    String TOPIC_5 = "fx.kafka.demo.hello.run5";

    /* for restart test end */

    /* for producer api test start */

    // in cluster 1
    String TOPIC_6 = "fx.kafka.demo.hello.run6";

    // in cluster 2
    String TOPIC_7 = "fx.kafka.demo.hello.run7";

    // in cluster 1
    String TOPIC_8 = "fx.kafka.demo.hello.run8";

    /* for producer api test end */

    /* for consumer api test start */

    // in cluster 1
    String TOPIC_9 = "fx.kafka.demo.hello.run9";

    // in cluster 2
    String TOPIC_10 = "fx.kafka.demo.hello.run10";

    // in cluster 1
    String TOPIC_11 = "fx.kafka.demo.hello.run11";

    /* for consumer api test end */

}
