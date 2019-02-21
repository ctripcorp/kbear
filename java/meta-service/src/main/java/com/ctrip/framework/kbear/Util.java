package com.ctrip.framework.kbear;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.mydotey.java.StringExtension;
import org.mydotey.rpc.ack.Acks;
import org.mydotey.rpc.error.ErrorCodes;
import org.slf4j.Logger;

import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.service.ResponseError;
import com.ctrip.framework.kbear.service.ResponseStatus;

/**
 * @author koqizhao
 *
 * Nov 15, 2018
 */
public interface Util {

    public static Map<String, String> toMap(Map<String, Object> map) {
        if (map == null)
            return new HashMap<>();

        return map.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> String.valueOf(e.getValue())));
    }

    public static boolean isEmpty(ConsumerGroupId consumerGroupId) {
        return consumerGroupId == null || StringExtension.isBlank(consumerGroupId.getGroupName())
                || StringExtension.isBlank(consumerGroupId.getTopicId());
    }

    public static void handleBadReqeust(ResponseStatus.Builder statusBuilder, Object request) {
        String errorMessage = String.format("request is bad: %s", request);
        statusBuilder.setAck(Acks.FAIL);
        statusBuilder
                .setError(ResponseError.newBuilder().setCode(ErrorCodes.BAD_REQUEST).setMessage(errorMessage).build());
    }

    public static void handleServiceException(ResponseStatus.Builder statusBuilder, Throwable ex, Logger logger) {
        String errorMessage = "service execution failed";
        logger.error(errorMessage, ex);
        statusBuilder.setAck(Acks.FAIL);
        statusBuilder.setError(
                ResponseError.newBuilder().setCode(ErrorCodes.SERVICE_EXCEPTION).setMessage(errorMessage).build());
    }

}
