package com.ctrip.framework.kbear.config;

import java.io.IOException;
import java.util.Map;

import org.mydotey.scf.yaml.YamlFileConfigurationSource;
import org.mydotey.scf.yaml.YamlFileConfigurationSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.TypedConfig;
import qunar.tc.qconfig.client.TypedConfig.Parser;

/**
 * @author koqizhao
 *
 * Nov 20, 2018
 */
public class YamlQConfigSource extends YamlFileConfigurationSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(YamlQConfigSource.class);

    public YamlQConfigSource(YamlFileConfigurationSourceConfig config) {
        super(config);
    }

    @Override
    protected Object loadYamlProperties() {
        Feature feature = Feature.create().setFailOnNotExists(false).build();
        TypedConfig<Map<Object, Object>> config = TypedConfig.get(getConfig().getFileName(), feature,
                new Parser<Map<Object, Object>>() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public Map<Object, Object> parse(String data) throws IOException {
                        if (data == null || data.trim().isEmpty())
                            return null;

                        try {
                            Object properties = new Yaml().load(data);
                            return checkSupported(properties) ? (Map<Object, Object>) properties : null;
                        } catch (Exception e) {
                            LOGGER.warn("failed to load yaml file: " + getConfig().getFileName(), e);
                            return null;
                        }
                    }
                });
        config.addListener(this::updateProperties);
        return config.current();
    }

    @Override
    protected void updateProperties(Map<Object, Object> properties) {
        if (properties == null) {
            LOGGER.error("Got null config, maybe something bad happen, skip update.");
            return;
        }

        super.updateProperties(properties);
    }

}
