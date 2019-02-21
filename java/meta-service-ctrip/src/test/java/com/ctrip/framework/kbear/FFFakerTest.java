package com.ctrip.framework.kbear;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.framework.foundation.Foundation;

/**
 * @author koqizhao
 *
 * Nov 30, 2018
 */
public class FFFakerTest {

    @Test
    public void fakeSubEnvTest() {
        String subEnv = Foundation.server().getSubEnv();
        String fake = "xxxx";

        FFFaker.fakeSubEnv(fake);
        String faked = Foundation.server().getSubEnv();
        Assert.assertEquals(fake, faked);

        FFFaker.fakeSubEnv(subEnv);
        faked = Foundation.server().getSubEnv();
        Assert.assertEquals(subEnv, faked);
    }

}
