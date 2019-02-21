package com.ctrip.framework.kbear.route;

public interface RouteService {

    FetchRouteRulesResponse fetchRouteRules(FetchRouteRulesRequest request);

    FetchProducerRouteResponse fetchProducerRoute(FetchProducerRouteRequest request);

    FetchConsumerRouteResponse fetchConsumerRoute(FetchConsumerRouteRequest request);

}
