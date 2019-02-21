package com.ctrip.framework.kbear.client;

/**
 * @author koqizhao
 *
 * Jan 10, 2019
 */
public class CProducerTest extends ProducerTest {

    @Override
    protected KafkaClientFactory newKafkaClientFactory() {
        return CKafkaClientFactory.getDefault();
    }

    @Override
    public void tearDown() {

    }

}
