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

This produces `target/rule-engine-1.0.0-custom-nodes.jar`.

## Adding the node to a ThingsBoard image

The node has to be on the rule engine's classpath. The packaged `thingsboard.conf` already sets

```sh
export LOADER_PATH=/usr/share/thingsboard/conf,/usr/share/thingsboard/extensions
```

so dropping the jar into `/usr/share/thingsboard/extensions/` is all that is required — no configuration
and no ThingsBoard code change.

The commands below use two variables. **Export them first**, in every shell you run these commands in —
the later steps reference them, and a plain assignment is lost when you open a new terminal:

```bash
export REPO=your_docker_repo        # e.g. the Docker Hub namespace you push to
export TAG=your_image_tag           # the tag of the ThingsBoard image you are extending
```

Against a stock release, the bundled [`Dockerfile`](Dockerfile) does exactly that:

```bash
DOCKER_BUILDKIT=0 docker build . -t $REPO/tb-node:4.3.1.3-custom-1
```

### On top of an image you build yourself

If you build ThingsBoard from source, the usual flow is:

```bash
cd <thingsboard-source>
mvn license:format clean install -DskipTests
docker buildx build -t $REPO/tb-node:$TAG \
  --platform=linux/amd64,linux/arm64 -o type=registry msa/tb-node/target
```

**Copying the jar into `msa/tb-node/target/` does not work.** That directory is only the build context;
the generated Dockerfile copies three named files (`logback.xml`, `start-tb-node.sh` and the `.deb`), so
a jar it does not reference is ignored.

Add a second, one-layer build on top of the image instead. ThingsBoard does not need rebuilding for
this — the node is a separate jar:

```bash
cd <this-project>
mvn clean install

cat > Dockerfile.node <<EOF
FROM $REPO/tb-node:$TAG
COPY target/rule-engine-1.0.0-custom-nodes.jar /usr/share/thingsboard/extensions/
EOF

docker buildx build -f Dockerfile.node \
  -t $REPO/tb-node:$TAG-split-to-queue \
  --platform=linux/amd64,linux/arm64 -o type=registry .
```

The jar is architecture-independent, so both platforms come from the same base manifest and no second
ThingsBoard build is needed.

Alternatively, append the `COPY` to the *generated* Dockerfile after `mvn install` and before `buildx`,
which keeps everything in one image and one build:

```bash
cp <this-project>/target/rule-engine-1.0.0-custom-nodes.jar msa/tb-node/target/
echo 'COPY rule-engine-1.0.0-custom-nodes.jar /usr/share/thingsboard/extensions/' >> msa/tb-node/target/Dockerfile
```

`msa/tb-node/target/` is regenerated on every build, so this leaves the ThingsBoard tree unmodified —
but it also has to be repeated on every rebuild, which is easy to forget.

### Notes

- Only the rule engine instantiates the node, but `tb-node` is the image that serves both core and rule
  engine. That is harmless: core simply never uses it.
- Keep `thingsboard.version` in [`pom.xml`](pom.xml) aligned with the image you are extending. The APIs
  this node uses are stable across patch releases, but compiling against the exact runtime removes any
  doubt.
