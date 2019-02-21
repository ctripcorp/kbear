package com.ctrip.framework.kbear.client;

import java.util.HashMap;
import java.util.Map;

import org.mydotey.java.StringExtension;
import org.mydotey.scf.ConfigurationSourceConfig;
import org.mydotey.scf.source.stringproperty.StringPropertyConfigurationSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;

/**
 * @author koqizhao
 *
 * Jan 3, 2019
 */
public class EnvConfigurationSource extends StringPropertyConfigurationSource<ConfigurationSourceConfig> {

    private static final Logger log = LoggerFactory.getLogger(EnvConfigurationSource.class);

    private static Map<Env, String> META_SERVICE_URLS = new HashMap<>();

    static {
        for (Env env : Env.values()) {
            String metaUrl;
            switch (env) {
                case PRO:
                    metaUrl = "http://meta.kafka.fx.ctripcorp.com";
                    break;
                case UAT:
                    metaUrl = "http://meta.kafka.fx.uat.qa.nt.ctripcorp.com";
                    break;
                default:
                    metaUrl = "http://meta.kafka.fx.fws.qa.nt.ctripcorp.com";
                    break;
            }
            META_SERVICE_URLS.put(env, metaUrl);
        }
    }

    private String _metaServiceUrl;

    public EnvConfigurationSource(ConfigurationSourceConfig config) {
        super(config);

        _metaServiceUrl = Foundation.metadata().getProperty("kafka.meta.url", StringExtension.EMPTY);
        if (StringExtension.isBlank(_metaServiceUrl))
            _metaServiceUrl = META_SERVICE_URLS.get(Foundation.server().getEnv());
        log.info("kafka meta service url is {}", _metaServiceUrl);
    }

    @Override
    public String getPropertyValue(String key) {
        switch (key) {
            case "kafka.meta.service.url":
                return _metaServiceUrl;
            default:
                return null;
        }
    }

}
