package com.ctrip.framework.kbear.rest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletResponse;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.mydotey.java.StringExtension;
import org.mydotey.rpc.ack.Acks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import com.ctrip.framework.kbear.route.Client;
import com.ctrip.framework.kbear.service.ResponseError;
import com.ctrip.framework.kbear.service.ResponseStatus;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

/**
 * @author koqizhao
 *
 * Dec 12, 2018
 */
@Configuration
@EnableAspectJAutoProxy
@Aspect
public class MonitoringConfig {

    private static Logger _logger = LoggerFactory.getLogger(MonitoringConfig.class);

    private static final String CAT_SUCCESS = "0";
    private static final String UNKNOWN = "unknown";

    @Pointcut("target(com.ctrip.framework.kbear.meta.MetaService) || target(com.ctrip.framework.kbear.route.RouteService)")
    private void service() {
    }

    @Pointcut("target(com.ctrip.framework.kbear.hermes.proxy.TopicResource) && @annotation(org.springframework.web.bind.annotation.RequestMapping)")
    private void hermesProxy() {
    }

    @Around("service()")
    private Object onServiceExecution(ProceedingJoinPoint joinPoint) throws Throwable {
        return runServiceInCat(joinPoint);
    }

    @Around("hermesProxy()")
    private Object onHermesProxyExecution(ProceedingJoinPoint joinPoint) throws Throwable {
        return runHermesProxyInCat(joinPoint);
    }

    private Object runServiceInCat(ProceedingJoinPoint joinPoint) throws Throwable {
        String type = joinPoint.getSignature().getDeclaringType().getInterfaces()[0].getSimpleName();
        String method = joinPoint.getSignature().getName();
        String typePrefix = type + ":" + method;
        Message request = (Message) joinPoint.getArgs()[0];
        logRequest(typePrefix, request);
        Transaction transaction = Cat.newTransaction(type, method);
        try {
            Message response = (Message) joinPoint.proceed();
            ResponseStatus status = getResponseStatus(response);
            logResponse(typePrefix, request, response, status);
            transaction.setStatus(toCatSuccess(status));
            return response;
        } catch (Throwable e) {
            transaction.setStatus(e);
            Cat.logError(e);
            throw e;
        } finally {
            transaction.complete();
        }
    }

    private Object runHermesProxyInCat(ProceedingJoinPoint joinPoint) throws Throwable {
        String type = "hermes:proxy:" + joinPoint.getSignature().getDeclaringType().getSimpleName();
        String method = joinPoint.getSignature().getName();
        String typePrefix = type + ":" + method;
        Object[] params = joinPoint.getArgs();
        AtomicReference<HttpServletResponse> servletResponse = new AtomicReference<>();
        List<Object> otherParams = new ArrayList<>();
        Arrays.stream(params).forEach(p -> {
            if (p instanceof HttpServletResponse)
                servletResponse.set((HttpServletResponse) p);
            else
                otherParams.add(p);
        });
        Transaction transaction = Cat.newTransaction(type, method);
        try {
            Object response = joinPoint.proceed();
            String ack = servletResponse.get().getStatus() < 500 ? Acks.SUCCESS : Acks.FAIL;
            String catSuccess = toCatSuccess(ack);
            logResponse(typePrefix, otherParams, servletResponse.get(), response, catSuccess);
            transaction.setStatus(catSuccess);
            return response;
        } catch (Throwable e) {
            transaction.setStatus(e);
            Cat.logError(e);
            throw e;
        } finally {
            transaction.complete();
        }
    }

    private ResponseStatus getResponseStatus(Message response) {
        if (response == null)
            return null;

        try {
            Method method = response.getClass().getMethod("getStatus");
            return (ResponseStatus) method.invoke(response);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException e) {
            _logger.error("method getStatus invocation failed, response: " + response, e);
            return null;
        }
    }

    private String getSizeEventName(int size) {
        if (size <= 0)
            return "0";
        if (size <= 4)
            return "(0, 4B]";
        if (size <= 16)
            return "(4B, 16B]";
        if (size <= 64)
            return "(16B, 64B]";
        if (size <= 256)
            return "(64B, 256B]";
        if (size <= 1024)
            return "(256B, 1k]";
        if (size <= 4 * 1024)
            return "(1k, 4k]";
        if (size <= 16 * 1024)
            return "(4k, 16k]";
        if (size <= 64 * 1024)
            return "(16k, 64k]";
        if (size <= 256 * 1024)
            return "(64k, 256k]";
        if (size <= 1024 * 1024)
            return "(256k, 1m]";
        if (size <= 4 * 1024 * 1024)
            return "(1m, 4m]";
        if (size <= 16 * 1024 * 1024)
            return "(4m, 16m]";
        return "> 16m";
    }

    private String toCatSuccess(ResponseStatus status) {
        return toCatSuccess(status == null ? null : status.getAck());
    }

    private String toCatSuccess(String ack) {
        return Acks.isFail(ack) ? ack : CAT_SUCCESS;
    }

    private void logRequest(String typePrefix, Message request) {
        int size = request == null ? 0 : request.getSerializedSize();
        Cat.logEvent(typePrefix + ":request:size", getSizeEventName(size), CAT_SUCCESS, "size=" + size);

        if (request == null)
            return;

        Method method = Arrays.stream(request.getClass().getMethods())
                .filter(m -> Objects.equals("getClient", m.getName())).findFirst().orElse(null);
        if (method == null)
            return;

        try {
            Client client = (Client) method.invoke(request);
            String clientId = client == null ? UNKNOWN : client.getId();
            if (StringExtension.isBlank(clientId))
                clientId = UNKNOWN;

            String type = typePrefix + ":request:clientId";
            Map<String, String> info = Objects.equals(clientId, UNKNOWN)
                    ? ImmutableMap.of("request", request.toString())
                    : null;
            Cat.logEvent(type, clientId, CAT_SUCCESS, info);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            _logger.error("method getClient invocation failed, request: " + request, e);
        }
    }

    private void logResponse(String typePrefix, Message request, Message response, ResponseStatus status) {
        int size = response == null ? 0 : response.getSerializedSize();
        Cat.logEvent(typePrefix + ":response:size", getSizeEventName(size), CAT_SUCCESS, "size=" + size);

        String requestString = StringExtension.toString(request);
        String responseString = StringExtension.toString(response);
        if (status == null) {
            _logger.error("response status is null, request: {}, response: {}", requestString, responseString);
            return;
        }

        String catSuccess = toCatSuccess(status);
        String ack = status.getAck() == null ? UNKNOWN : status.getAck();
        Map<String, String> data = Acks.isFail(ack) || Objects.equals(ack, UNKNOWN)
                ? ImmutableMap.of("request", requestString, "response", responseString)
                : null;
        Cat.logEvent(typePrefix + ":response:ack", ack, catSuccess, data);
        ResponseError error = status.getError();
        if (error != null && !StringExtension.isBlank(error.getCode()))
            Cat.logEvent(typePrefix + ":response:error", error.getCode(), catSuccess,
                    ImmutableMap.of("message", error.getMessage() == null ? UNKNOWN : error.getMessage()));
    }

    private void logResponse(String typePrefix, List<Object> params, HttpServletResponse servletResponse,
            Object response, String catSuccess) {
        String requestString = StringExtension.toString(params);
        String responseString = StringExtension.toString(response);
        Map<String, String> data = servletResponse.getStatus() >= 300
                ? ImmutableMap.of("request", requestString, "response", responseString)
                : null;
        Cat.logEvent(typePrefix + ":response:ack", String.valueOf(servletResponse.getStatus()), catSuccess, data);
    }

}
