package com.ctrip.framework.kbear.rest.resource;

import javax.inject.Inject;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.framework.kbear.rest.RestConfig;
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
 * Sep 21, 2018
 */
@RequestMapping(path = "/route", method = RequestMethod.POST, consumes = { MediaType.APPLICATION_JSON_VALUE,
        RestConfig.APPLICATION_PROTOBUF_VALUE })
@RestController
public class RouteServiceResource {

    @Inject
    private RouteService _service;

    @RequestMapping(path = "/rules/all", method = RequestMethod.GET, consumes = "*/*", produces = "*/*")
    public FetchRouteRulesResponse fetchRouteRule() {
        return _service.fetchRouteRules(null);
    }

    @RequestMapping("/rules")
    public FetchRouteRulesResponse fetchRouteRule(@RequestBody FetchRouteRulesRequest request) {
        return _service.fetchRouteRules(request);
    }

    @RequestMapping("/producer")
    public FetchProducerRouteResponse fetchProducerRoute(@RequestBody FetchProducerRouteRequest request) {
        return _service.fetchProducerRoute(request);
    }

    @RequestMapping("/consumer")
    public FetchConsumerRouteResponse fetchConsumerRoute(@RequestBody FetchConsumerRouteRequest request) {
        return _service.fetchConsumerRoute(request);
    }

}
