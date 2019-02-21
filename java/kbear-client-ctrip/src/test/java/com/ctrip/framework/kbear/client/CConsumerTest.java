package com.ctrip.framework.kbear.client;

/**
 * @author koqizhao
 *
 * Jan 10, 2019
 */
public class CConsumerTest extends ConsumerTest {

    @Override
    protected KafkaClientFactory newKafkaClientFactory() {
        return CKafkaClientFactory.getDefault();
    }

    @Override
    public void tearDown() {

    }

}
