/**
 * Copyright 2018 ThingsBoard, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.rule.engine.node.transform;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.ScriptEngine;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.msg.TbNodeConnectionType;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.script.ScriptLanguage;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Splits one incoming message into many with a script, re-originates each produced message to an entity
 * looked up by name, and enqueues each one directly onto a configured queue.
 *
 * <p>The built-in script node cannot do this. When its script returns more than one message it re-enqueues
 * every one of them onto the queue the incoming message arrived on, so a fan-out of N amplifies that queue
 * N-fold and the produced messages are keyed by the ORIGINAL originator — collapsing partition spread to the
 * number of distinct callers. Reaching a different queue then costs a second enqueue per message, and
 * re-originating costs another node.
 *
 * <p>This node performs all three steps in one pass: split, re-originate, enqueue to the target queue. Each
 * produced message is therefore partitioned by its own originator, and crosses the queue once instead of twice.
 *
 * <p>The incoming message is forwarded to <b>Success</b> only after EVERY produced message has been
 * successfully enqueued, and is routed to <b>Failure</b> if any one of them fails — so a partial fan-out is
 * never silently committed.
 *
 * <p>Both the incoming message and the produced messages leave on <b>Success</b>: the incoming one directly,
 * the produced ones when they are later consumed from the target queue. They are distinguishable by message
 * type, so a following switch can send each down its own branch.
 */
@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "split to queue",
        configClazz = TbSplitToQueueNodeConfiguration.class,
        hasQueueName = true,
        nodeDescription = "Splits a message with a script and enqueues each produced message to the configured queue, re-originated to an entity found by name.",
        nodeDetails = "The script must return an ARRAY of messages, in the same shape the <code>script</code> " +
                "transformation node accepts: <code>[{msg: ..., metadata: ..., msgType: ...}, ...]</code>.<br><br>" +
                "For every produced message the configured name pattern is resolved against that message, the " +
                "entity of the configured type is looked up by the resulting name, and it becomes the message's " +
                "originator. The message is then enqueued directly onto the queue configured on this node.<br><br>" +
                "The incoming message is routed to <code>Success</code> only once all produced messages are " +
                "enqueued. If any of them fails to enqueue, or an entity cannot be found, the incoming message " +
                "is routed to <code>Failure</code> instead and nothing is enqueued.<br><br>" +
                "Produced messages also arrive on <code>Success</code>, when they are consumed from the target " +
                "queue. Since they keep the message type the script gave them, a <code>message type switch</code> " +
                "placed after this node can route them separately from the incoming message.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>."
)
public class TbSplitToQueueNode implements TbNode {

    private TbSplitToQueueNodeConfiguration config;
    private ScriptEngine scriptEngine;
    private String targetQueue;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        config = TbNodeUtils.convert(configuration, TbSplitToQueueNodeConfiguration.class);
        if (config.getOriginatorType() == null) {
            throw new TbNodeException("Originator type is not set!", true);
        }
        // Only devices are resolvable by name here. Rejected at configuration time rather than per message.
        if (config.getOriginatorType() != EntityType.DEVICE) {
            throw new TbNodeException("Unsupported originator type: " + config.getOriginatorType(), true);
        }
        if (config.getOriginatorNamePattern() == null || config.getOriginatorNamePattern().isBlank()) {
            throw new TbNodeException("Originator name pattern is not set!", true);
        }
        ScriptLanguage lang = config.getScriptLang() == null ? ScriptLanguage.TBEL : config.getScriptLang();
        String script = ScriptLanguage.TBEL.equals(lang) ? config.getTbelScript() : config.getJsScript();
        if (script == null || script.isBlank()) {
            throw new TbNodeException("Script is not set!", true);
        }
        scriptEngine = ctx.createScriptEngine(lang, script);
        // The queue this node enqueues to. Configured on the node itself (hasQueueName), not inherited from
        // the incoming message — that is the whole point of the node.
        targetQueue = ctx.getQueueName();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        Futures.addCallback(scriptEngine.executeUpdateAsync(msg), new FutureCallback<>() {
            @Override
            public void onSuccess(List<TbMsg> produced) {
                enqueueAll(ctx, msg, produced);
            }

            @Override
            public void onFailure(Throwable t) {
                ctx.tellFailure(msg, t);
            }
        }, MoreExecutors.directExecutor());
    }

    private void enqueueAll(TbContext ctx, TbMsg incoming, List<TbMsg> produced) {
        if (produced == null || produced.isEmpty()) {
            ctx.tellFailure(incoming, new TbNodeException("Script produced no messages!"));
            return;
        }
        // Resolve every originator BEFORE enqueueing anything: a name that cannot be resolved must fail the
        // whole incoming message rather than leave part of the fan-out already on the queue.
        //
        // The lookups run concurrently. findDeviceByTenantIdAndNameAsync submits the CACHED lookup to the
        // DAO's own executor, so this neither loses the cache nor needs an executor of its own — resolving a
        // large fan-out costs roughly one lookup's latency instead of the sum of all of them.
        List<String> names = new ArrayList<>(produced.size());
        List<ListenableFuture<Device>> lookups = new ArrayList<>(produced.size());
        for (TbMsg out : produced) {
            String name = TbNodeUtils.processPattern(config.getOriginatorNamePattern(), out);
            if (name == null || name.isBlank()) {
                ctx.tellFailure(incoming, new TbNodeException("Originator name pattern resolved to an empty name!"));
                return;
            }
            names.add(name);
            lookups.add(ctx.getDeviceService().findDeviceByTenantIdAndNameAsync(ctx.getTenantId(), name));
        }

        // allAsList fails as soon as any lookup fails, so a partial resolution never reaches the enqueue below.
        Futures.addCallback(Futures.allAsList(lookups), new FutureCallback<>() {
            @Override
            public void onSuccess(List<Device> devices) {
                TbMsg[] toEnqueue = new TbMsg[produced.size()];
                for (int i = 0; i < produced.size(); i++) {
                    Device device = devices.get(i);
                    if (device == null) { // not found — the lookup resolves to null rather than failing
                        ctx.tellFailure(incoming, new TbNodeException(
                                "Failed to find " + config.getOriginatorType().name().toLowerCase()
                                        + " with name '" + names.get(i) + "'!"));
                        return;
                    }
                    toEnqueue[i] = produced.get(i).transform().originator(device.getId()).build();
                }
                enqueueResolved(ctx, incoming, toEnqueue);
            }

            @Override
            public void onFailure(Throwable t) {
                ctx.tellFailure(incoming, t);
            }
        }, MoreExecutors.directExecutor());
    }

    private void enqueueResolved(TbContext ctx, TbMsg incoming, TbMsg[] toEnqueue) {
        // Forward the incoming message only once every produced message is enqueued; fail it on the first
        // enqueue error. `failed` makes the failure path fire at most once.
        AtomicInteger pending = new AtomicInteger(toEnqueue.length);
        AtomicBoolean failed = new AtomicBoolean();
        for (TbMsg out : toEnqueue) {
            ctx.enqueueForTellNext(out, targetQueue, TbNodeConnectionType.SUCCESS,
                    () -> {
                        if (pending.decrementAndGet() == 0 && !failed.get()) {
                            ctx.tellSuccess(incoming);
                        }
                    },
                    error -> {
                        if (failed.compareAndSet(false, true)) {
                            ctx.tellFailure(incoming, error);
                        }
                    });
        }
    }

    @Override
    public void destroy() {
        if (scriptEngine != null) {
            scriptEngine.destroy();
            scriptEngine = null;
        }
    }

}
