package com.ctrip.framework.kbear.client;

/**
 * @author koqizhao
 *
 * Jan 25, 2019
 */
public class CBasicTest extends BasicTest {

    @Override
    protected KafkaClientFactory newKafkaClientFactory() {
        return CKafkaClientFactory.getDefault();
    }

    @Override
    public void tearDown() {

    }

}
