package com.ctrip.framework.kbear.service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.mydotey.rpc.ack.Acks;
import org.mydotey.rpc.error.ErrorCodes;
import org.springframework.test.context.TestContextManager;

@RunWith(Parameterized.class)
public abstract class ServiceTest {

    protected static final ResponseStatus SUCCESS = ResponseStatus.newBuilder().setAck(Acks.SUCCESS).build();
    protected static final ResponseStatus PARTIAL_FAIL = ResponseStatus.newBuilder().setAck(Acks.PARTIAL_FAIL).build();
    protected static final ResponseStatus FAIL_SERVICE_EXCEPTION = ResponseStatus.newBuilder().setAck(Acks.FAIL)
            .setError(ResponseError.newBuilder().setCode(ErrorCodes.SERVICE_EXCEPTION)
                    .setMessage("service execution failed").build())
            .build();

    @Parameter(0)
    public Object _request;

    @Parameter(1)
    public Object _response;

    @Parameter(2)
    public Method _method;

    @Before
    public void setUp() throws Exception {
        if (useContextManager())
            new TestContextManager(getClass()).prepareTestInstance(this);
    }

    @Test
    public void invoke() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Object response = _method.invoke(getService(), _request);
        Assert.assertEquals(_response, response);
    }

    protected abstract boolean useContextManager();

    protected abstract Object getService();

}
