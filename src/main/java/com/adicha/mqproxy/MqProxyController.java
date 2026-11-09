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
 * <p>Required headers:
 * <pre>
 *   qm_name   queue manager name
 *   mq_in     queue the request is sent to
 *   mq_out    queue the reply is read from
 * </pre>
 *
 * <p>Optional headers (leave mq_ip out to talk to the local queue manager in bindings mode):
 * <pre>
 *   mq_ip      host of the queue manager
 *   mq_port    listener port (default 1414)
 *   mq_channel server-connection channel (required when mq_ip is set)
 *   mq_user    user id (omit for an anonymous connection)
 *   mq_pass    password
 * </pre>
 *
 * <p>Body: the JSON payload, forwarded as-is. The reply body is returned as-is.
 *
 * <p>Examples:
 * <pre>
 * # remote queue manager
 * curl -X POST http://localhost:8080/mq \
 *   -H 'Content-Type: application/json' \
 *   -H 'qm_name: QM1' -H 'mq_in: DEV.REQUEST.QUEUE' -H 'mq_out: DEV.REPLY.QUEUE' \
 *   -H 'mq_ip: 10.0.0.5' -H 'mq_port: 1414' -H 'mq_channel: DEV.APP.SVRCONN' \
 *   -H 'mq_user: app' -H 'mq_pass: passw0rd' \
 *   -d '{"customerId":"123"}'
 *
 * # local queue manager, bindings mode
 * curl -X POST http://localhost:8080/mq \
 *   -H 'Content-Type: application/json' \
 *   -H 'qm_name: QM1' -H 'mq_in: DEV.REQUEST.QUEUE' -H 'mq_out: DEV.REPLY.QUEUE' \
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
            @RequestHeader("qm_name") String queueManager,
            @RequestHeader("mq_in") String inQueue,
            @RequestHeader("mq_out") String outQueue,
            @RequestHeader(name = "mq_ip", required = false) String host,
            @RequestHeader(name = "mq_port", required = false) Integer port,
            @RequestHeader(name = "mq_channel", required = false) String channel,
            @RequestHeader(name = "mq_user", required = false) String user,
            @RequestHeader(name = "mq_pass", required = false) String password,
            @RequestBody(required = false) String payload) {

        MqProxyService.MqRequest request = new MqProxyService.MqRequest(
                host, port, channel, queueManager, inQueue, outQueue, user, password, payload);

        // MQ calls are blocking -> run them off the event loop
        return Mono.fromCallable(() -> mqProxyService.roundTrip(request))
                .subscribeOn(Schedulers.boundedElastic())
                .map(body -> ResponseEntity.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(body))
                .onErrorResume(MqProxyService.ReplyTimeoutException.class,
                        e -> Mono.just(plain(HttpStatus.GATEWAY_TIMEOUT, e)))
                .onErrorResume(e -> Mono.just(plain(HttpStatus.BAD_GATEWAY, e)));
    }

    private static ResponseEntity<String> plain(HttpStatus status, Throwable e) {
        String message = e.getMessage() == null ? e.toString() : e.getMessage();
        return ResponseEntity.status(status).contentType(MediaType.TEXT_PLAIN).body(message);
    }
}
