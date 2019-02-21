package com.ctrip.framework.kbear.rest.resource;

import javax.inject.Inject;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.framework.kbear.meta.FetchClustersRequest;
import com.ctrip.framework.kbear.meta.FetchClustersResponse;
import com.ctrip.framework.kbear.meta.FetchConsumerGroupsRequest;
import com.ctrip.framework.kbear.meta.FetchConsumerGroupsResponse;
import com.ctrip.framework.kbear.meta.FetchTopicsRequest;
import com.ctrip.framework.kbear.meta.FetchTopicsResponse;
import com.ctrip.framework.kbear.meta.MetaService;
import com.ctrip.framework.kbear.rest.RestConfig;

/**
 * @author koqizhao
 *
 * Sep 21, 2018
 */
@RequestMapping(path = "/meta", method = RequestMethod.POST, consumes = { MediaType.APPLICATION_JSON_VALUE,
        RestConfig.APPLICATION_PROTOBUF_VALUE })
@RestController
public class MetaServiceResource {

    @Inject
    private MetaService _service;

    @RequestMapping(path = "/clusters/all", method = RequestMethod.GET, consumes = "*/*", produces = "*/*")
    public FetchClustersResponse fetchClusters() {
        return _service.fetchClusters(null);
    }

    @RequestMapping("/clusters")
    public FetchClustersResponse fetchClusters(@RequestBody FetchClustersRequest request) {
        return _service.fetchClusters(request);
    }

    @RequestMapping(path = "/topics/all", method = RequestMethod.GET, consumes = "*/*", produces = "*/*")
    public FetchTopicsResponse fetchTopics() {
        return _service.fetchTopics(null);
    }

    @RequestMapping("/topics")
    public FetchTopicsResponse fetchTopics(@RequestBody FetchTopicsRequest request) {
        return _service.fetchTopics(request);
    }

    @RequestMapping(path = "/consumer-groups/all", method = RequestMethod.GET, consumes = "*/*", produces = "*/*")
    public FetchConsumerGroupsResponse fetchConsumerGroups() {
        return _service.fetchConsumerGroups(null);
    }

    @RequestMapping("/consumer-groups")
    public FetchConsumerGroupsResponse fetchConsumerGroups(@RequestBody FetchConsumerGroupsRequest request) {
        return _service.fetchConsumerGroups(request);
    }

}
