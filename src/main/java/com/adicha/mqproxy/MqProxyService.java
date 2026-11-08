package com.adicha.mqproxy;

import com.ibm.mq.MQC;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Hashtable;

/**
 * The whole flow lives here:
 *
 * <pre>
 *   1. connect to the queue manager (host/port/channel/qm/user/pass from the HTTP headers)
 *   2. open the reply queue FIRST (so a fast reply can never be missed)
 *   3. put the JSON payload on the request queue, let MQ generate the MsgId
 *   4. get from the reply queue with MQMO_MATCH_CORREL_ID = that MsgId, waiting up to N ms
 *   5. return the reply body, close everything
 * </pre>
 *
 * A fresh connection is used per HTTP request (simple, no pooling) - fine for a test tool.
 */
@Service
public class MqProxyService {

    private static final Logger log = LoggerFactory.getLogger(MqProxyService.class);

    /** CCSID for UTF-8, so MQ knows how to convert the body if the consumer needs it. */
    private static final int UTF8_CCSID = 1208;

    private final int replyTimeoutMs;
    private final String defaultUser;
    private final String defaultPassword;
    private final String defaultRequestQueue;
    private final String defaultReplyQueue;
    private final String modelQueue;
    private final String dynamicQueueName;

    public MqProxyService(
            @Value("${mq.reply-timeout-ms:30000}") int replyTimeoutMs,
            @Value("${mq.user:}") String defaultUser,
            @Value("${mq.password:}") String defaultPassword,
            @Value("${mq.request-queue:}") String defaultRequestQueue,
            @Value("${mq.reply-queue:}") String defaultReplyQueue,
            @Value("${mq.model-queue:SYSTEM.DEFAULT.MODEL.QUEUE}") String modelQueue,
            @Value("${mq.dynamic-queue-name:MQPROXY.REPLY.*}") String dynamicQueueName) {
        this.replyTimeoutMs = replyTimeoutMs;
        this.defaultUser = defaultUser;
        this.defaultPassword = defaultPassword;
        this.defaultRequestQueue = defaultRequestQueue;
        this.defaultReplyQueue = defaultReplyQueue;
        this.modelQueue = modelQueue;
        this.dynamicQueueName = dynamicQueueName;
    }

    /** Everything the proxy needs for one round trip. */
    public record MqRequest(String host,
                            int port,
                            String channel,
                            String queueManager,
                            String user,
                            String password,
                            String requestQueue,
                            String replyQueue,
                            String payload) {
    }

    /** Thrown when no reply arrives with our correlation id within the wait window. */
    public static class ReplyTimeoutException extends RuntimeException {
        public ReplyTimeoutException(String message) {
            super(message);
        }
    }

    public String roundTrip(MqRequest req) throws MQException, IOException {
        String requestQueueName = firstNonBlank(req.requestQueue(), defaultRequestQueue);
        if (requestQueueName == null) {
            throw new IllegalArgumentException("mq_queue header (or mq.request-queue property) is required");
        }

        MQQueueManager queueManager = null;
        MQQueue requestQueue = null;
        MQQueue replyQueue = null;
        try {
            // 1) connect
            queueManager = openQueueManager(req);

            // 2) open / create the reply queue before sending anything
            String replyQueueName = firstNonBlank(req.replyQueue(), defaultReplyQueue);
            if (replyQueueName != null) {
                replyQueue = queueManager.accessQueue(replyQueueName,
                        MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_FAIL_IF_QUIESCING);
            } else {
                // no reply queue given -> temporary dynamic queue, removed again on close
                replyQueue = queueManager.accessQueue(modelQueue,
                        MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_FAIL_IF_QUIESCING,
                        null, dynamicQueueName, null);
                replyQueueName = replyQueue.getName(); // MQ resolves the "*" part
            }
            log.info("REPLY queue = {}", replyQueueName);

            requestQueue = queueManager.accessQueue(requestQueueName,
                    MQC.MQOO_OUTPUT | MQC.MQOO_FAIL_IF_QUIESCING);

            // 3) send the payload
            MQMessage request = new MQMessage();
            request.format = MQC.MQFMT_STRING;
            request.characterSet = UTF8_CCSID;
            request.replyToQueueName = replyQueueName;
            request.writeString(req.payload() == null ? "" : req.payload());
            requestQueue.put(request, new MQPutMessageOptions());

            byte[] msgId = request.messageId; // filled in by MQ
            log.info("PUT {} ({} chars) msgId={}", requestQueueName, request.getDataLength(), toHex(msgId));

            // 4) wait for the reply whose CorrelId equals our MsgId
            MQMessage reply = new MQMessage();
            reply.correlationId = msgId;

            MQGetMessageOptions gmo = new MQGetMessageOptions();
            gmo.options = MQC.MQGMO_WAIT | MQC.MQGMO_FAIL_IF_QUIESCING;
            gmo.matchOptions = MQC.MQMO_MATCH_CORREL_ID;
            gmo.waitInterval = replyTimeoutMs;

            try {
                replyQueue.get(reply, gmo);
            } catch (MQException e) {
                if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                    throw new ReplyTimeoutException(
                            "No reply for msgId " + toHex(msgId) + " within " + replyTimeoutMs + " ms");
                }
                throw e;
            }

            // 5) hand the reply body back untouched
            byte[] body = new byte[reply.getDataLength()];
            reply.readFully(body);
            log.info("GOT reply on {} ({} bytes) correlId={}", replyQueueName, body.length, toHex(reply.correlationId));
            return new String(body, StandardCharsets.UTF_8);
        } finally {
            closeQuietly(replyQueue);
            closeQuietly(requestQueue);
            if (queueManager != null) {
                try {
                    queueManager.disconnect();
                } catch (MQException e) {
                    log.warn("disconnect failed: {}", e.getMessage());
                }
            }
        }
    }

    private MQQueueManager openQueueManager(MqRequest req) throws MQException {
        Hashtable<String, Object> props = new Hashtable<>();
        props.put(MQC.HOST_NAME_PROPERTY, req.host());
        props.put(MQC.PORT_PROPERTY, req.port());
        props.put(MQC.CHANNEL_PROPERTY, req.channel());
        props.put(MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_CLIENT);

        // Credentials are optional: on a local/test queue manager you can leave
        // mq_user / mq_pass out and the channel's MCAUSER / CHLAUTH decides.
        String user = firstNonBlank(req.user(), defaultUser);
        if (user != null) {
            String password = firstNonBlank(req.password(), defaultPassword);
            props.put(MQC.USER_ID_PROPERTY, user);
            props.put(MQC.PASSWORD_PROPERTY, password == null ? "" : password);
        }
        log.info("CONNECT {}:{} channel={} qm={} user={}",
                req.host(), req.port(), req.channel(), req.queueManager(), user == null ? "<none>" : user);
        return new MQQueueManager(req.queueManager(), props);
    }

    private static void closeQuietly(MQQueue queue) {
        if (queue != null) {
            try {
                queue.close();
            } catch (MQException e) {
                log.warn("close failed: {}", e.getMessage());
            }
        }
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v.trim();
            }
        }
        return null;
    }

    private static String toHex(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }
}
