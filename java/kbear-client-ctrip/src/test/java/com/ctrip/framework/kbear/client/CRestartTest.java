package com.ctrip.framework.kbear.client;

import org.mydotey.scf.ConfigurationManager;

import com.ctrip.framework.kbear.route.Client;

/**
 * @author koqizhao
 *
 * Jan 29, 2019
 */
public class CRestartTest extends RestartTest {

    @Override
    protected KafkaClientFactory newKafkaClientFactory() {
        Client client = CKafkaClientFactory.newDefaultClient();
        ConfigurationManager configurationManager = newConfigurationManager();
        client.setId("test");
        client.getMeta().put("ok", "ok_value");
        return new CKafkaClientFactory(configurationManager, client);
    }

}
