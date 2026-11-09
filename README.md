# mqproxy

Thin HTTP -> IBM MQ request/reply proxy.

`POST /mq` puts your JSON payload on an MQ queue, then waits (30s by default) for the reply message whose
**CorrelId equals the request's MsgId**, and returns that reply body unchanged.

```
HTTP POST /mq  --payload-->  mq_in   (request queue)
              <--reply----   mq_out  (reply queue, matched on CorrelId = MsgId)
```

The MQ responder must follow the normal MQ reply pattern: copy the request **MsgId into CorrelId** of the
reply (JMS: `reply.setJMSCorrelationID(request.getJMSMessageID())`).

## Run

```bash
./mvnw spring-boot:run          # Linux/macOS/Git Bash
mvnw.cmd spring-boot:run        # Windows cmd
```

App listens on `http://localhost:8080` (override with `server.port`).

## Headers

| Header | Required | Default | Meaning |
|---|---|---|---|
| `qm_name` | yes | – | queue manager name |
| `mq_in` | yes | – | queue the request is sent to |
| `mq_out` | yes | – | queue the reply is read from |
| `mq_ip` | no | *empty* | queue manager host. **Empty = local queue manager (bindings mode)** |
| `mq_port` | no | `1414` | listener port |
| `mq_channel` | with `mq_ip` | – | server-connection channel |
| `mq_user` | no | *empty* | user id. Empty = anonymous connection (channel `MCAUSER`/CHLAUTH decides) |
| `mq_pass` | no | *empty* | password |
| `meta_<name>` | no | – | every header starting with the literal prefix `meta_` (case-sensitive) is copied into the MQ message as property `<name>` |

### `meta_` headers -> MQ message properties

Any request header whose name starts with `meta_` (exact case) is added to the message as a **string
property**. The prefix is stripped:

```
meta_sourceSystem: BILLING   ->  MQ property sourceSystem = BILLING
meta_traceId: 9f8c...        ->  MQ property traceId     = 9f8c...
Meta_ignored: x              ->  not copied (uppercase M)
```

Notes:

* Property names are unqualified, so the MQ Java classes store them in the RFH2 `usr.` folder
  (`usr.sourceSystem`). A JMS consumer reads them back as plain `sourceSystem`.
* Values are always strings. Repeated headers are joined with `,`.
* When at least one `meta_` header is sent, MQ wraps the payload in an MQRFH2 and the message format
  becomes `MQHRF2` instead of `MQSTR` — relevant if the consumer parses the header itself.

Server-side default (in `application.properties`):

| Property | Default | Meaning |
|---|---|---|
| `mq.reply-timeout-ms` | `30000` | how long to wait for the reply |
| `server.port` | `8080` | HTTP port |

Body: the JSON payload, forwarded as-is. Content type is irrelevant.

## curl examples

### 1. Local queue manager, no credentials (bindings mode)

Only `qm_name`, `mq_in`, `mq_out` are sent — no ip, no port, no channel, no user/pass:

```bash
curl -X POST http://localhost:8080/mq \
  -H 'Content-Type: application/json' \
  -H 'qm_name: QM1' \
  -H 'mq_in: DEV.REQUEST.QUEUE' \
  -H 'mq_out: DEV.REPLY.QUEUE' \
  -d '{"customerId":"123","action":"GET_BALANCE"}'
```

### 2. Remote queue manager, anonymous (no user/pass)

`mq_port` omitted -> 1414:

```bash
curl -X POST http://localhost:8080/mq \
  -H 'Content-Type: application/json' \
  -H 'qm_name: QM1' \
  -H 'mq_in: DEV.REQUEST.QUEUE' \
  -H 'mq_out: DEV.REPLY.QUEUE' \
  -H 'mq_ip: 10.0.0.5' \
  -H 'mq_channel: DEV.APP.SVRCONN' \
  -d '{"customerId":"123","action":"GET_BALANCE"}'
```

### 3. Remote queue manager with credentials and explicit port

```bash
curl -X POST http://localhost:8080/mq \
  -H 'Content-Type: application/json' \
  -H 'qm_name: QM1' \
  -H 'mq_in: DEV.REQUEST.QUEUE' \
  -H 'mq_out: DEV.REPLY.QUEUE' \
  -H 'mq_ip: 10.0.0.5' \
  -H 'mq_port: 1414' \
  -H 'mq_channel: DEV.APP.SVRCONN' \
  -H 'mq_user: app' \
  -H 'mq_pass: passw0rd' \
  -d '{"customerId":"123","action":"GET_BALANCE"}'
```

### 4. "Local" over TCP instead of bindings

Same machine, but you want a normal client connection (no MQ client install needed):

```bash
curl -X POST http://localhost:8080/mq \
  -H 'Content-Type: application/json' \
  -H 'qm_name: QM1' \
  -H 'mq_in: DEV.REQUEST.QUEUE' \
  -H 'mq_out: DEV.REPLY.QUEUE' \
  -H 'mq_ip: 127.0.0.1' \
  -H 'mq_channel: DEV.APP.SVRCONN' \
  -d '{"customerId":"123"}'
```

### 5. Payload from a file, verbose output

```bash
curl -v -X POST http://localhost:8080/mq \
  -H 'Content-Type: application/json' \
  -H 'qm_name: QM1' -H 'mq_in: DEV.REQUEST.QUEUE' -H 'mq_out: DEV.REPLY.QUEUE' \
  --data-binary @request.json
```

### 6. With MQ message properties from `meta_` headers

```bash
curl -X POST http://localhost:8080/mq \
  -H 'Content-Type: application/json' \
  -H 'qm_name: QM1' -H 'mq_in: DEV.REQUEST.QUEUE' -H 'mq_out: DEV.REPLY.QUEUE' \
  -H 'meta_sourceSystem: BILLING' \
  -H 'meta_traceId: 9f8c1d77' \
  -d '{"customerId":"123"}'
```

The message on `DEV.REQUEST.QUEUE` then carries the properties `sourceSystem` and `traceId`.

### 7. Windows PowerShell / cmd

In PowerShell `curl` is an alias for `Invoke-WebRequest`; use `curl.exe`:

```powershell
curl.exe -X POST http://localhost:8080/mq `
  -H "Content-Type: application/json" `
  -H "qm_name: QM1" `
  -H "mq_in: DEV.REQUEST.QUEUE" `
  -H "mq_out: DEV.REPLY.QUEUE" `
  -d '{\"customerId\":\"123\"}'
```

## Docker

Build:

```bash
docker build -t mqproxy:latest .
```

Run (map the port; the proxy itself is stateless, all MQ settings come from the request headers):

```bash
docker run --rm -p 8080:8080 --name mqproxy mqproxy:latest
```

Then call it exactly like the curl examples above:

```bash
curl -X POST http://localhost:8080/mq \
  -H 'Content-Type: application/json' \
  -H 'qm_name: QM1' -H 'mq_in: DEV.REQUEST.QUEUE' -H 'mq_out: DEV.REPLY.QUEUE' \
  -H 'mq_ip: 10.0.0.5' -H 'mq_channel: DEV.APP.SVRCONN' \
  -d '{"customerId":"123"}'
```

Notes for Docker:

* The image is a two-stage build on `maven:3.9-eclipse-temurin-21` + `eclipse-temurin:21-jre`.
* **Bindings mode does not work in the container** (no local queue manager / client install), so always
  send `mq_ip` (use `host.docker.internal` or the container name to reach MQ from Docker).
* HTTP port can be changed with `-e SERVER_PORT=9090 -p 9090:9090`.

## Responses

| Status | Meaning |
|---|---|
| `200` | reply received; body is the raw reply message |
| `504` | no message with matching CorrelId arrived within `mq.reply-timeout-ms` |
| `502` | MQ failure (connect, auth, queue not found, ...); body is the MQ reason text |
| `400` | missing required header |

## Local MQ notes

If a connection is rejected with `MQRC 2035` (not authorized), the queue manager is refusing the
anonymous client. For a throwaway local queue manager:

```
runmqsc QM1
ALTER QMGR CHLAUTH(DISABLED)
REFRESH SECURITY(*) TYPE(CONNAUTH)
```

Bindings mode (`mq_ip` absent) uses the queue manager on the machine running this app and needs a full
IBM MQ client installation (native `mqjbnd` library). If you get an unsupported/native-library error,
pass `mq_ip: 127.0.0.1` instead and it will connect over TCP.

## Logs

Each call logs the flow:

```
CONNECT 10.0.0.5:1414 channel=DEV.APP.SVRCONN qm=QM1 user=app
PUT msgId=414D... properties=[sourceSystem, traceId]
GOT reply (87 bytes) correlId=414D...
```
