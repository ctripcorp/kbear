package com.ctrip.framework.kbear.rest.resource;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.ctrip.framework.kbear.rest.App;
import com.ctrip.framework.kbear.route.RouteServiceTest;

/**
 * @author koqizhao
 *
 * Nov 29, 2018
 */
public class RouteServiceResourceTest extends RouteServiceTest {

    private RouteServiceClient _routeServiceClient;

    private ConfigurableApplicationContext _applicationContext;

    @Before
    public void setUp() {
        _applicationContext = SpringApplication.run(App.class);
        _routeServiceClient = new RouteServiceClient("http://localhost:8081");
    }

    @After
    public void tearDown() throws IOException {
        _applicationContext.close();
        _routeServiceClient.close();
    }

    @Override
    protected boolean useContextManager() {
        return false;
    }

    @Override
    protected Object getService() {
        return _routeServiceClient;
    }

}
