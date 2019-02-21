package com.ctrip.framework.kbear.rest.resource;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;

import com.ctrip.framework.kbear.route.FetchConsumerRouteRequest;
import com.ctrip.framework.kbear.route.FetchConsumerRouteResponse;
import com.ctrip.framework.kbear.route.FetchProducerRouteRequest;
import com.ctrip.framework.kbear.route.FetchProducerRouteResponse;
import com.ctrip.framework.kbear.route.FetchRouteRulesRequest;
import com.ctrip.framework.kbear.route.FetchRouteRulesResponse;
import com.ctrip.framework.kbear.route.RouteService;

/**
 * @author koqizhao
 *
 * Nov 29, 2018
 */
public class RouteServiceClient implements RouteService, Closeable {

    private static final String FETCH_ROUTE_RULES_PROCEDURE = "route/fetchRouteRules";
    private static final String FETCH_ALL_ROUTE_RULES_PROCEDURE = "route/fetchRouteRules/all";
    private static final String FETCH_PRODUCER_ROUTE_PROCEDURE = "route/fetchProducerRoute";
    private static final String FETCH_CONSUMER_ROUTE_PROCEDURE = "route/fetchConsumerRoute";

    private static final HashMap<String, String> PROCEDURE_REST_PATH_MAP = new HashMap<>();

    static {
        PROCEDURE_REST_PATH_MAP.put(FETCH_ROUTE_RULES_PROCEDURE, "/route/rules");
        PROCEDURE_REST_PATH_MAP.put(FETCH_ALL_ROUTE_RULES_PROCEDURE, "/route/rules/all");
        PROCEDURE_REST_PATH_MAP.put(FETCH_PRODUCER_ROUTE_PROCEDURE, "/route/producer");
        PROCEDURE_REST_PATH_MAP.put(FETCH_CONSUMER_ROUTE_PROCEDURE, "/route/consumer");
    }

    private TestRpcClient _rpcClient;

    public RouteServiceClient(String serviceUrl) {
        _rpcClient = new TestRpcClient(serviceUrl, PROCEDURE_REST_PATH_MAP);
    }

    @Override
    public FetchRouteRulesResponse fetchRouteRules(FetchRouteRulesRequest request) {
        boolean emptyRequest = request.getRouteRuleIdsList().isEmpty();
        String procedure = emptyRequest ? FETCH_ALL_ROUTE_RULES_PROCEDURE : FETCH_ROUTE_RULES_PROCEDURE;
        return _rpcClient.invoke(procedure, emptyRequest ? null : request, FetchRouteRulesResponse.class);
    }

    @Override
    public FetchProducerRouteResponse fetchProducerRoute(FetchProducerRouteRequest request) {
        return _rpcClient.invoke(FETCH_PRODUCER_ROUTE_PROCEDURE, request, FetchProducerRouteResponse.class);
    }

    @Override
    public FetchConsumerRouteResponse fetchConsumerRoute(FetchConsumerRouteRequest request) {
        return _rpcClient.invoke(FETCH_CONSUMER_ROUTE_PROCEDURE, request, FetchConsumerRouteResponse.class);
    }

    @Override
    public void close() throws IOException {
        _rpcClient.close();
    }

}
