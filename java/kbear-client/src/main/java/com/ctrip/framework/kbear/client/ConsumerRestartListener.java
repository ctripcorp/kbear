package com.ctrip.framework.kbear.client;

import org.apache.kafka.clients.consumer.Consumer;

import com.ctrip.framework.kbear.meta.ConsumerGroupId;

/**
 * @author koqizhao
 *
 * Jan 23, 2019
 */
public interface ConsumerRestartListener<K, V> {

    void beforeRestart(ConsumerGroupId consumerGroupId, Consumer<K, V> consumer);

    void afterRestart(ConsumerGroupId consumerGroupId, Consumer<K, V> consumer);

}
