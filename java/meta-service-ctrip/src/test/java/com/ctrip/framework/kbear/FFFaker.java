package com.ctrip.framework.kbear;

import java.lang.reflect.Field;
import java.util.Objects;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.framework.foundation.internals.provider.DefaultServerProvider;

/**
 * @author koqizhao
 *
 * Nov 30, 2018
 */
public interface FFFaker {

    String SUB_ENV = "fat666";

    static void fake() {
        fakeSubEnv(SUB_ENV);
    }

    static void fakeSubEnv(String fake) {
        try {
            String subEnv = Foundation.server().getSubEnv();
            if (Objects.equals(fake, subEnv))
                return;

            System.out.printf("real subEnv: %s\n", subEnv);
            Field field = DefaultServerProvider.class.getDeclaredField("m_subEnv");
            field.setAccessible(true);
            field.set(Foundation.server(), fake);
            subEnv = Foundation.server().getSubEnv();
            System.out.printf("faked subEnv: %s\n", subEnv);
        } catch (Exception e) {
            throw new RuntimeException("subEnv fake failed", e);
        }
    }

}
