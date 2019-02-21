package com.ctrip.framework.kbear.client;

import java.lang.management.ManagementFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.common.utils.Sanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author koqizhao
 *
 * Jan 28, 2019
 */
class KafkaAppInfoExtension {

    private static Logger _logger = LoggerFactory.getLogger(KafkaAppInfoExtension.class);

    public static synchronized void unregister(String prefix, String clientId) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + Sanitizer.jmxSanitize(clientId));
            if (server.isRegistered(name))
                server.unregisterMBean(name);
        } catch (JMException e) {
            _logger.warn("Error unregistering AppInfo mbean", e);
        }
    }

}
