package com.ctrip.framework.kbear.repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.mydotey.scf.ConfigurationManager;
import org.mydotey.scf.ConfigurationManagerConfig;
import org.mydotey.scf.Property;
import org.mydotey.scf.PropertyConfig;
import org.mydotey.scf.facade.ConfigurationManagers;
import org.mydotey.scf.facade.ConfigurationProperties;
import org.mydotey.scf.type.TypeConverter;
import org.mydotey.scf.yaml.YamlFileConfigurationSource;
import org.mydotey.scf.yaml.YamlFileConfigurationSourceConfig;

/**
 * @author koqizhao
 *
 * Nov 19, 2018
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public abstract class AbstractConfigRepository<Key, Model> implements Repository<Key, Model> {

    private ConfigurationManager _configurationManager;
    private volatile List<Model> _records;

    public AbstractConfigRepository() {
        _configurationManager = newConfigurationManager();
        init();
    }

    @Override
    public List<Model> getAll() {
        return _records;
    }

    protected abstract String getRepositoryName();

    protected abstract TypeConverter<List<Map<String, Object>>, List<Model>> getTypeConverter();

    protected abstract Function<List<Model>, List<Model>> getValueFilter();

    protected ConfigurationManager getConfigurationManager() {
        return _configurationManager;
    }

    protected ConfigurationManager newConfigurationManager() {
        String repositoryName = getRepositoryName();
        YamlFileConfigurationSourceConfig yamlFileConfigurationSourceConfig = new YamlFileConfigurationSourceConfig.Builder()
                .setFileName(repositoryName + ".yaml").setName("yaml-local-file").build();
        YamlFileConfigurationSource yamlFileConfigurationSource = new YamlFileConfigurationSource(
                yamlFileConfigurationSourceConfig);
        ConfigurationManagerConfig managerConfig = ConfigurationManagers.newConfigBuilder()
                .setName(repositoryName + "-repository").addSource(1, yamlFileConfigurationSource).build();
        return ConfigurationManagers.newManager(managerConfig);
    };

    protected void init() {
        String repositoryName = getRepositoryName();
        PropertyConfig.Builder<String, List<Model>> builder = ConfigurationProperties.newConfigBuilder();
        builder.setKey(repositoryName).setValueType((Class) List.class).setDefaultValue(new ArrayList<Model>())
                .addValueConverter(getTypeConverter()).setValueFilter(getValueFilter());
        Property<String, List<Model>> property = getConfigurationManager().getProperty(builder.build());
        _records = Collections.unmodifiableList(property.getValue());
        property.addChangeListener(e -> {
            if (!e.getNewValue().isEmpty())
                _records = Collections.unmodifiableList(e.getNewValue());
        });
    }

}
