package com.adicha.mqproxy;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Single endpoint: POST /mq
 *
 * <p>All headers are optional and fall back to the {@code mq.*} properties;
 * the only real requirements are a queue manager name and a request queue.
 * <pre>
 *   mq_ip          host of the queue manager  (missing/empty -> local bindings mode)
 *   mq_port        listener port             (default 1414)
 *   mq_channel     server-connection channel (required for a remote mq_ip)
 *   qm_name        queue manager name        (default mq.queue-manager)
 *   mq_user        user id                   (optional -> anonymous connection)
 *   mq_pass        password                  (optional -> anonymous connection)
 *   mq_queue       request queue to put to   (default mq.request-queue)
 *   mq_reply_queue reply queue to read from  (optional -> temp dynamic queue is used)
 * </pre>
 *
 * <p>Body: the JSON payload, forwarded as-is. The reply body is returned as-is.
 *
 * <p>Examples:
 * <pre>
 * # remote queue manager
 * curl -X POST http://localhost:8080/mq \
 *   -H 'Content-Type: application/json' \
 *   -H 'mq_ip: 10.0.0.5'      -H 'mq_port: 1414' \
 *   -H 'mq_channel: DEV.APP.SVRCONN' -H 'qm_name: QM1' \
 *   -H 'mq_user: app'         -H 'mq_pass: passw0rd' \
 *   -H 'mq_queue: DEV.REQUEST.QUEUE' \
 *   -d '{"customerId":"123"}'
 *
 * # local queue manager, bindings mode (no ip/port/channel/user/pass)
 * curl -X POST http://localhost:8080/mq \
 *   -H 'Content-Type: application/json' \
 *   -H 'qm_name: QM1' -H 'mq_queue: DEV.REQUEST.QUEUE' \
 *   -d '{"customerId":"123"}'
 * </pre>
 */
@RestController
public class MqProxyController {

    private final MqProxyService mqProxyService;

    public MqProxyController(MqProxyService mqProxyService) {
        this.mqProxyService = mqProxyService;
    }

    @PostMapping(path = "/mq", consumes = MediaType.ALL_VALUE)
    public Mono<ResponseEntity<String>> proxy(
            @RequestHeader(name = "mq_ip", required = false) String host,
            @RequestHeader(name = "mq_port", required = false) Integer port,
            @RequestHeader(name = "mq_channel", required = false) String channel,
            @RequestHeader(name = "qm_name", required = false) String queueManager,
            @RequestHeader(name = "mq_user", required = false) String user,
            @RequestHeader(name = "mq_pass", required = false) String password,
            @RequestHeader(name = "mq_queue", required = false) String requestQueue,
            @RequestHeader(name = "mq_reply_queue", required = false) String replyQueue,
            @RequestBody(required = false) String payload) {

        MqProxyService.MqRequest request = new MqProxyService.MqRequest(
                host, port, channel, queueManager, user, password,
                requestQueue, replyQueue, payload);

        // MQ calls are blocking -> run them off the event loop
        return Mono.fromCallable(() -> mqProxyService.roundTrip(request))
                .subscribeOn(Schedulers.boundedElastic())
                .map(body -> ResponseEntity.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(body))
                .onErrorResume(MqProxyService.ReplyTimeoutException.class, e -> Mono.just(plain(HttpStatus.GATEWAY_TIMEOUT, e)))
                .onErrorResume(e -> Mono.just(plain(HttpStatus.BAD_GATEWAY, e)));
    }

    private static ResponseEntity<String> plain(HttpStatus status, Throwable e) {
        String message = e.getMessage() == null ? e.toString() : e.getMessage();
        return ResponseEntity.status(status).contentType(MediaType.TEXT_PLAIN).body(message);
    }
}
