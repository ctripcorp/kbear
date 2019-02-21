package com.ctrip.framework.kbear.client;

import com.ctrip.framework.kbear.route.Client;

/**
 * @author koqizhao
 *
 * Jan 25, 2019
 */
public class CBasicTest2 extends BasicTest {

    @Override
    protected KafkaClientFactory newKafkaClientFactory() {
        Client client = CKafkaClientFactory.newDefaultClient();
        client.setId("test");
        client.getMeta().put("ok", "ok_value");
        return new CKafkaClientFactory(client);
    }

}
