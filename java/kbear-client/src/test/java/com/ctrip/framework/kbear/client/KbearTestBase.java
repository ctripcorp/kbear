package com.ctrip.framework.kbear.client;

import org.junit.After;
import org.junit.Before;
import org.mydotey.scf.ConfigurationManager;
import org.mydotey.scf.ConfigurationManagerConfig;
import org.mydotey.scf.facade.ConfigurationManagers;
import org.mydotey.scf.facade.StringPropertySources;
import org.mydotey.scf.source.stringproperty.environmentvariable.EnvironmentVariableConfigurationSource;
import org.mydotey.scf.source.stringproperty.memorymap.MemoryMapConfigurationSource;
import org.mydotey.scf.source.stringproperty.systemproperties.SystemPropertiesConfigurationSource;

/**
 * @author koqizhao
 *
 * Jan 23, 2019
 */
public abstract class KbearTestBase {

    private KafkaClientFactory _clientFactory;

    protected KafkaClientFactory getClientFactory() {
        return _clientFactory;
    }

    protected KafkaClientFactory newKafkaClientFactory() {
        ConfigurationManager configurationManager = newConfigurationManager();
        KafkaMetaManager kafkaMetaManager = new DefaultKafkaMetaManager(configurationManager);
        return new DefaultKafkaClientFactory(configurationManager, kafkaMetaManager);
    }

    protected ConfigurationManager newConfigurationManager() {
        MemoryMapConfigurationSource configurationSource = StringPropertySources.newMemoryMapSource("memory");
        configurationSource.setPropertyValue(TestData.META_SERVICE_PROPERTY_KEY, TestData.META_SERVICE_URL);
        EnvironmentVariableConfigurationSource configurationSource2 = StringPropertySources
                .newEnvironmentVariableSource("env");
        SystemPropertiesConfigurationSource configurationSource3 = StringPropertySources
                .newSystemPropertiesSource("system");
        ConfigurationManagerConfig managerConfig = ConfigurationManagers.newConfigBuilder()
                .setName("kbear-kafka-client").addSource(1, configurationSource).addSource(2, configurationSource2)
                .addSource(3, configurationSource3).build();
        return ConfigurationManagers.newManager(managerConfig);
    }

    @Before
    public void setUp() {
        _clientFactory = newKafkaClientFactory();
    }

    @After
    public void tearDown() throws Exception {
        _clientFactory.close();
    }

}
