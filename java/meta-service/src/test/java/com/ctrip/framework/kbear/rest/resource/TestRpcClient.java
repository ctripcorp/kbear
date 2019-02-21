package com.ctrip.framework.kbear.rest.resource;

import java.io.IOException;
import java.util.Map;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.mydotey.codec.Codec;
import org.mydotey.codec.json.ProtoMessageJsonCodec;
import org.mydotey.rpc.client.http.apache.ApacheHttpRpcClient;
import org.mydotey.rpc.client.http.apache.HttpRequestFactory;

/**
 * @author koqizhao
 *
 * Nov 29, 2018
 */
public class TestRpcClient extends ApacheHttpRpcClient {

    private String _serviceUrl;
    private Map<String, String> _procedureRestPathMap;

    private CloseableHttpClient _syncClient;
    private CloseableHttpAsyncClient _asyncClient;

    public TestRpcClient(String serviceUrl, Map<String, String> procedureRestPathMap) {
        _serviceUrl = serviceUrl;
        _procedureRestPathMap = procedureRestPathMap;

        _syncClient = HttpClients.createDefault();
        _asyncClient = HttpAsyncClients.createDefault();
    }

    @Override
    public void close() throws IOException {
        _syncClient.close();
        _asyncClient.close();
    }

    @Override
    protected CloseableHttpClient getHttpClient() {
        return _syncClient;
    }

    @Override
    protected CloseableHttpAsyncClient getHttpAsyncClient() {
        return _asyncClient;
    }

    @Override
    protected <Req> HttpUriRequest toHttpUriRequest(String procedure, Req request) {
        String restPath = _procedureRestPathMap.get(procedure);
        if (restPath == null)
            throw new IllegalArgumentException("unknown procedure: " + procedure);

        String requestUrl = _serviceUrl + restPath;
        if (request == null)
            return HttpRequestFactory.createRequest(requestUrl, HttpGet.METHOD_NAME);
        return HttpRequestFactory.createRequest(requestUrl, HttpPost.METHOD_NAME, request, getCodec());
    }

    @Override
    protected Codec getCodec() {
        return ProtoMessageJsonCodec.DEFAULT;
    }

}
