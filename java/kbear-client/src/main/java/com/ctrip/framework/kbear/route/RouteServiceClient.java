package com.ctrip.framework.kbear.route;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.mydotey.java.ObjectExtension;
import org.mydotey.rpc.client.http.HttpServiceClient;
import org.mydotey.rpc.client.http.HttpServiceClientConfig;

/**
 * @author koqizhao
 *
 * Dec 18, 2018
 */
public class RouteServiceClient implements RouteService, AutoCloseable {

    private static final String FETCH_ROUTE_RULES_PROCEDURE = "route/fetchRouteRules";
    private static final String FETCH_PRODUCER_ROUTE_PROCEDURE = "route/fetchProducerRoute";
    private static final String FETCH_CONSUMER_ROUTE_PROCEDURE = "route/fetchConsumerRoute";

    public static final Map<String, String> PROCEDURE_REST_PATH_MAP;

    static {
        HashMap<String, String> map = new HashMap<>();
        map.put(FETCH_ROUTE_RULES_PROCEDURE, "/route/rules");
        map.put(FETCH_PRODUCER_ROUTE_PROCEDURE, "/route/producer");
        map.put(FETCH_CONSUMER_ROUTE_PROCEDURE, "/route/consumer");

        PROCEDURE_REST_PATH_MAP = Collections.unmodifiableMap(map);
    }

    private HttpServiceClient _serviceClient;

    public RouteServiceClient(HttpServiceClientConfig config) {
        ObjectExtension.requireNonNull(config, "config");
        _serviceClient = new HttpServiceClient(config);
    }

    public HttpServiceClientConfig getConfig() {
        return _serviceClient.getConfig();
    }

    @Override
    public FetchRouteRulesResponse fetchRouteRules(FetchRouteRulesRequest request) {
        return _serviceClient.invoke(FETCH_ROUTE_RULES_PROCEDURE, request, FetchRouteRulesResponse.class);
    }

    @Override
    public FetchProducerRouteResponse fetchProducerRoute(FetchProducerRouteRequest request) {
        return _serviceClient.invoke(FETCH_PRODUCER_ROUTE_PROCEDURE, request, FetchProducerRouteResponse.class);
    }

    @Override
    public FetchConsumerRouteResponse fetchConsumerRoute(FetchConsumerRouteRequest request) {
        return _serviceClient.invoke(FETCH_CONSUMER_ROUTE_PROCEDURE, request, FetchConsumerRouteResponse.class);
    }

    @Override
    public void close() throws Exception {
        _serviceClient.close();
    }

}
