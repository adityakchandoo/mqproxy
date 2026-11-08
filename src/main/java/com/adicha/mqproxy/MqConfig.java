package com.adicha.mqproxy;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Defaults for everything the caller may leave out of the HTTP headers.
 * Any of these can be set in application.properties, e.g. {@code mq.channel=DEV.APP.SVRCONN}.
 *
 * <p>{@code mq.host} empty means "local queue manager, bindings mode".
 */
@ConfigurationProperties(prefix = "mq")
public record MqConfig(
        String host,
        Integer port,
        String channel,
        String queueManager,
        String user,
        String password,
        String requestQueue,
        String replyQueue,
        String modelQueue,
        String dynamicQueueName,
        Integer replyTimeoutMs) {

    public MqConfig {
        port = (port == null) ? 1414 : port;
        replyTimeoutMs = (replyTimeoutMs == null) ? 30000 : replyTimeoutMs;
        modelQueue = isBlank(modelQueue) ? "SYSTEM.DEFAULT.MODEL.QUEUE" : modelQueue;
        dynamicQueueName = isBlank(dynamicQueueName) ? "MQPROXY.REPLY.*" : dynamicQueueName;
    }

    static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
