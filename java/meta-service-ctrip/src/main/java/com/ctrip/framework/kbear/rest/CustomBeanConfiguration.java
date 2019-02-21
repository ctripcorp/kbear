package com.ctrip.framework.kbear.rest;

import javax.inject.Inject;
import javax.inject.Named;

import org.mydotey.scf.ConfigurationManager;
import org.mydotey.scf.ConfigurationManagerConfig;
import org.mydotey.scf.facade.ConfigurationManagers;
import org.mydotey.scf.yaml.YamlFileConfigurationSourceConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.ctrip.framework.kbear.config.YamlQConfigSource;
import com.ctrip.framework.kbear.meta.repository.ClusterRepository;
import com.ctrip.framework.kbear.meta.repository.ConsumerGroupRepository;
import com.ctrip.framework.kbear.meta.repository.QConfigClusterRepository;
import com.ctrip.framework.kbear.meta.repository.QConfigConsumerGroupRepository;
import com.ctrip.framework.kbear.meta.repository.QConfigTopicRepository;
import com.ctrip.framework.kbear.meta.repository.TopicRepository;
import com.ctrip.framework.kbear.route.repository.QConfigRouteRuleRepository;
import com.ctrip.framework.kbear.route.repository.RouteRuleRepository;

/**
 * @author koqizhao
 *
 * Nov 20, 2018
 */
@Configuration
public class CustomBeanConfiguration {

    public static final String BEAN_APP_CONFIG = "app-config";

    @Inject
    @Named(QConfigRouteRuleRepository.BEAN_NAME)
    private RouteRuleRepository _routeRuleRepository;

    @Bean
    @Primary
    public RouteRuleRepository routeRuleRepository() {
        return _routeRuleRepository;
    }

    @Inject
    @Named(QConfigTopicRepository.BEAN_NAME)
    private TopicRepository _topicRepository;

    @Bean
    @Primary
    public TopicRepository topicRepository() {
        return _topicRepository;
    }

    @Inject
    @Named(QConfigClusterRepository.BEAN_NAME)
    private ClusterRepository _clusterRepository;

    @Bean
    @Primary
    public ClusterRepository clusterRepository() {
        return _clusterRepository;
    }

    @Inject
    @Named(QConfigConsumerGroupRepository.BEAN_NAME)
    private ConsumerGroupRepository _consumerGroupRepository;

    @Bean
    @Primary
    public ConsumerGroupRepository consumerGroupRepository() {
        return _consumerGroupRepository;
    }

    @Bean
    @Named(BEAN_APP_CONFIG)
    public ConfigurationManager appConfig() {
        String fileName = "app";
        YamlFileConfigurationSourceConfig yamlQConfigSourceConfig = new YamlFileConfigurationSourceConfig.Builder()
                .setFileName(fileName + ".yml").setName("yaml-qconfig-file").build();
        YamlQConfigSource yamlQConfigSource = new YamlQConfigSource(yamlQConfigSourceConfig);
        ConfigurationManagerConfig managerConfig = ConfigurationManagers.newConfigBuilder().setName(BEAN_APP_CONFIG)
                .addSource(1, yamlQConfigSource).build();
        return ConfigurationManagers.newManager(managerConfig);
    }
}
