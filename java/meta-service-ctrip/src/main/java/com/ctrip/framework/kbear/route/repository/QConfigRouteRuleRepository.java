package com.ctrip.framework.kbear.route.repository;

import javax.inject.Named;
import javax.inject.Singleton;

import org.mydotey.scf.ConfigurationManager;

import com.ctrip.framework.kbear.config.Util;

/**
 * @author koqizhao
 *
 * Nov 13, 2018
 */
@Singleton
@Named(QConfigRouteRuleRepository.BEAN_NAME)
public class QConfigRouteRuleRepository extends ConfigRouteRuleRepository {

    public static final String BEAN_NAME = "qconfig-route-rule-repository";

    @Override
    protected ConfigurationManager newConfigurationManager() {
        String repositoryName = getRepositoryName();
        return Util.newConfigurationManager(repositoryName);
    }

}
