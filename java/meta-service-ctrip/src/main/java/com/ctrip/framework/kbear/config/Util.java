package com.ctrip.framework.kbear.config;

import org.mydotey.scf.ConfigurationManager;
import org.mydotey.scf.ConfigurationManagerConfig;
import org.mydotey.scf.facade.ConfigurationManagers;
import org.mydotey.scf.yaml.YamlFileConfigurationSourceConfig;

/**
 * @author koqizhao
 *
 * Nov 20, 2018
 */
public interface Util {

    static ConfigurationManager newConfigurationManager(String fileName) {
        YamlFileConfigurationSourceConfig yamlQConfigSourceConfig = new YamlFileConfigurationSourceConfig.Builder()
                .setFileName(fileName + ".yml").setName("yaml-qconfig-file").build();
        YamlQConfigSource yamlQConfigSource = new YamlQConfigSource(yamlQConfigSourceConfig);
        ConfigurationManagerConfig managerConfig = ConfigurationManagers.newConfigBuilder()
                .setName(fileName + "-repository").addSource(2, yamlQConfigSource).build();
        return ConfigurationManagers.newManager(managerConfig);
    }

}
