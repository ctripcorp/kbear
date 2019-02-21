package com.ctrip.framework.kbear.meta.repository;

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
@Named(QConfigClusterRepository.BEAN_NAME)
public class QConfigClusterRepository extends ConfigClusterRepository {

    public static final String BEAN_NAME = "qconfig-cluster-repository";

    @Override
    protected ConfigurationManager newConfigurationManager() {
        String repositoryName = getRepositoryName();
        return Util.newConfigurationManager(repositoryName);
    }

}
