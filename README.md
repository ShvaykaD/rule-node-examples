# split-to-queue rule node

A custom ThingsBoard rule node that splits one message into many with a script, re-originates each
produced message to an entity looked up by name, and enqueues each one **directly onto a configured
queue** — all in a single pass.

## Why

The built-in `script` transformation node cannot do this. When its script returns more than one
message it re-enqueues every one of them onto the queue the incoming message arrived on. Two
consequences follow for a fan-out of N:

- **The source queue is amplified N-fold.** A message that fans out to 500 puts 500 more messages on
  the queue it came from, so the ingestion queue carries the fan-out rather than the arrival rate.
- **Partition spread collapses.** The produced messages are keyed by the *original* originator, so
  the number of partitions that receive work equals the number of distinct callers, not the
  configured partition count.

Reaching a different queue then costs a second enqueue per message — a second full queue transit,
with its own replication and its own producer batching delay — and re-originating costs another node
after that.

This node does the split, the re-originate and the enqueue in one step. Each produced message is
partitioned by its own originator, and crosses a queue once instead of twice.

## Behaviour

- The script must return an **array** of messages, in the same shape the built-in `script` node
  accepts: `[{msg: ..., metadata: ..., msgType: ...}, ...]`.
- For each produced message, `originatorNamePattern` is resolved **against that message**, an entity
  of `originatorType` is looked up by the resulting name, and it becomes that message's originator.
- Every produced message is enqueued onto the queue configured on the node itself (the node declares
  `hasQueueName`), not the queue the incoming message arrived on.
- The incoming message is acknowledged **only after every produced message has been enqueued**, and
  is routed to `Failure` if any one of them fails. A partial fan-out is never silently committed.
- All originators are resolved **before** anything is enqueued, so an unresolvable name fails the
  whole batch rather than leaving part of it on the queue.

Output connections: `Success`, `Failure`.

## Configuration

| field | meaning |
|---|---|
| `scriptLang` | `TBEL` or `JS` |
| `tbelScript` / `jsScript` | the split script; must return an array of messages |
| `originatorType` | entity type of the new originator (currently `DEVICE`) |
| `originatorNamePattern` | pattern resolved per produced message, e.g. `${entityName}` |

The **target queue** is not part of this configuration — it is the node's own queue name, set
alongside the node in the rule chain.

There is no UI configuration form for this node; it is configured through the rule-chain JSON or the
REST API.

## Build

```bash
mvn clean install
```

To build a ThingsBoard image with the node included:

```bash
DOCKER_BUILDKIT=0 docker build . -t your_repo/tb-node:4.3.1.3-custom-1
```
