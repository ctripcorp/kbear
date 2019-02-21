package com.ctrip.framework.kbear.client;

import org.mydotey.scf.ConfigurationManager;
import com.ctrip.framework.kbear.route.Client;

/**
 * @author koqizhao
 *
 * Jan 2, 2019
 */
public class CKafkaMetaManager extends DefaultKafkaMetaManager {

    public CKafkaMetaManager(ConfigurationManager configurationManager, Client client) {
        super(configurationManager, client);
    }

    public String getClientId() {
        return super.getClient().getId();
    }

}
