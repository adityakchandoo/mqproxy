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
import java.util.Map;

/**
 * One request/reply round trip, top to bottom:
 *
 * <pre>
 *   connect()  -> bindings mode if mq_ip is empty, else client connection
 *   open       -> mq_out (reply queue) first, so a fast reply can never be missed
 *                 mq_in  (request queue)
 *   send()     -> put the JSON payload, MQ generates the MsgId
 *   receive()  -> get from mq_out matching CorrelId = that MsgId, wait up to mq.reply-timeout-ms
 *   close      -> queue(s) + disconnect
 * </pre>
 *
 * A fresh connection per HTTP request; no pooling. Fine for a test tool.
 */
@Service
public class MqProxyService {

    private static final Logger log = LoggerFactory.getLogger(MqProxyService.class);

    /** CCSID for UTF-8. */
    private static final int UTF8_CCSID = 1208;

    private final int replyTimeoutMs;

    public MqProxyService(@Value("${mq.reply-timeout-ms:30000}") int replyTimeoutMs) {
        this.replyTimeoutMs = replyTimeoutMs;
    }

    /** The HTTP headers, as-is. host/port/channel/user/password may be null. */
    public record MqRequest(String host,
                            Integer port,
                            String channel,
                            String queueManager,
                            String inQueue,   // mq_in  : we send the request here
                            String outQueue,  // mq_out : we read the reply here
                            String user,
                            String password,
                            Map<String, String> meta, // meta_* headers -> MQ message properties
                            String payload) {
    }

    /** No reply with our correlation id within the wait window. */
    public static class ReplyTimeoutException extends RuntimeException {
        public ReplyTimeoutException(String message) {
            super(message);
        }
    }

    public String roundTrip(MqRequest req) throws MQException, IOException {
        MQQueueManager queueManager = connect(req);
        MQQueue outQueue = null;
        MQQueue inQueue = null;
        try {
            // open the reply queue before sending anything
            outQueue = queueManager.accessQueue(req.outQueue(),
                    MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_FAIL_IF_QUIESCING);
            inQueue = queueManager.accessQueue(req.inQueue(),
                    MQC.MQOO_OUTPUT | MQC.MQOO_FAIL_IF_QUIESCING);

            byte[] msgId = send(inQueue, req);
            return receive(outQueue, msgId);
        } finally {
            closeQuietly(outQueue);
            closeQuietly(inQueue);
            try {
                queueManager.disconnect();
            } catch (MQException e) {
                log.warn("disconnect failed: {}", e.getMessage());
            }
        }
    }

    /** Puts the payload and returns the MsgId that MQ assigned to it. */
    private byte[] send(MQQueue queue, MqRequest req) throws MQException, IOException {
        MQMessage message = new MQMessage();
        message.format = MQC.MQFMT_STRING;
        message.characterSet = UTF8_CCSID;
        message.replyToQueueName = req.outQueue();
        message.writeString(req.payload() == null ? "" : req.payload());

        // meta_* headers become MQ message properties (RFH2 usr. folder)
        for (Map.Entry<String, String> meta : req.meta().entrySet()) {
            message.setStringProperty(meta.getKey(), meta.getValue());
        }

        queue.put(message, new MQPutMessageOptions());
        log.info("PUT msgId={} properties={}", toHex(message.messageId), req.meta().keySet());
        return message.messageId;
    }

    /** Waits for the message on mq_out whose CorrelId equals the request MsgId. */
    private String receive(MQQueue queue, byte[] msgId) throws MQException, IOException {
        MQMessage reply = new MQMessage();
        reply.correlationId = msgId;

        MQGetMessageOptions options = new MQGetMessageOptions();
        options.options = MQC.MQGMO_WAIT | MQC.MQGMO_FAIL_IF_QUIESCING;
        options.matchOptions = MQC.MQMO_MATCH_CORREL_ID;
        options.waitInterval = replyTimeoutMs;

        try {
            queue.get(reply, options);
        } catch (MQException e) {
            if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                throw new ReplyTimeoutException(
                        "No reply with correlId " + toHex(msgId) + " within " + replyTimeoutMs + " ms");
            }
            throw e;
        }

        byte[] body = new byte[reply.getDataLength()];
        reply.readFully(body);
        log.info("GOT reply ({} bytes) correlId={}", body.length, toHex(reply.correlationId));
        return new String(body, StandardCharsets.UTF_8);
    }

    /** No mq_ip -> local queue manager (bindings mode). Otherwise a TCP client connection. */
    private MQQueueManager connect(MqRequest req) throws MQException {
        if (isBlank(req.host())) {
            log.info("CONNECT bindings (local) qm={}", req.queueManager());
            return new MQQueueManager(req.queueManager());
        }

        if (isBlank(req.channel())) {
            throw new IllegalArgumentException("mq_channel header is required when mq_ip is set");
        }

        Hashtable<String, Object> props = new Hashtable<>();
        props.put(MQC.HOST_NAME_PROPERTY, req.host());
        props.put(MQC.PORT_PROPERTY, req.port() == null ? 1414 : req.port());
        props.put(MQC.CHANNEL_PROPERTY, req.channel());
        props.put(MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_CLIENT);
        // credentials optional: without them the channel's MCAUSER / CHLAUTH decides
        if (!isBlank(req.user())) {
            props.put(MQC.USER_ID_PROPERTY, req.user());
            props.put(MQC.PASSWORD_PROPERTY, req.password() == null ? "" : req.password());
        }

        log.info("CONNECT {}:{} channel={} qm={} user={}",
                req.host(), props.get(MQC.PORT_PROPERTY), req.channel(), req.queueManager(),
                isBlank(req.user()) ? "<none>" : req.user());
        return new MQQueueManager(req.queueManager(), props);
    }

    private static void closeQuietly(MQQueue queue) {
        if (queue == null) {
            return;
        }
        try {
            queue.close();
        } catch (MQException e) {
            log.warn("close failed: {}", e.getMessage());
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
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
