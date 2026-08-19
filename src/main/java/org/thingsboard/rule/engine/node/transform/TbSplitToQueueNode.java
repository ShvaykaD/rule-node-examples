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
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.ScriptEngine;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.msg.TbNodeConnectionType;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.script.ScriptLanguage;
import org.thingsboard.server.common.msg.TbMsg;

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
 * <p>The incoming message is acknowledged only after EVERY produced message has been successfully enqueued,
 * and is failed if any one of them fails — so a partial fan-out is never silently committed.
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
                "The incoming message is acknowledged only once all produced messages are enqueued. If any of " +
                "them fails to enqueue, or an entity cannot be found, the incoming message is routed to " +
                "<code>Failure</code>.<br><br>" +
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
        TbMsg[] toEnqueue = new TbMsg[produced.size()];
        for (int i = 0; i < produced.size(); i++) {
            TbMsg out = produced.get(i);
            String name = TbNodeUtils.processPattern(config.getOriginatorNamePattern(), out);
            EntityId originator;
            try {
                originator = findEntityByName(ctx, config.getOriginatorType(), name);
            } catch (Exception e) {
                ctx.tellFailure(incoming, e);
                return;
            }
            toEnqueue[i] = out.transform().originator(originator).build();
        }

        // Acknowledge the incoming message only once every produced message is enqueued; fail it on the first
        // enqueue error. `failed` makes the failure path fire at most once.
        AtomicInteger pending = new AtomicInteger(toEnqueue.length);
        AtomicBoolean failed = new AtomicBoolean();
        for (TbMsg out : toEnqueue) {
            ctx.enqueueForTellNext(out, targetQueue, TbNodeConnectionType.SUCCESS,
                    () -> {
                        if (pending.decrementAndGet() == 0 && !failed.get()) {
                            ctx.ack(incoming);
                        }
                    },
                    error -> {
                        if (failed.compareAndSet(false, true)) {
                            ctx.tellFailure(incoming, error);
                        }
                    });
        }
    }

    private EntityId findEntityByName(TbContext ctx, EntityType type, String name) throws TbNodeException {
        if (name == null || name.isBlank()) {
            throw new TbNodeException("Originator name pattern resolved to an empty name!");
        }
        if (type != EntityType.DEVICE) {
            throw new TbNodeException("Unsupported originator type: " + type);
        }
        var device = ctx.getDeviceService().findDeviceByTenantIdAndName(ctx.getTenantId(), name);
        if (device == null) {
            throw new TbNodeException("Failed to find " + type.name().toLowerCase() + " with name '" + name + "'!");
        }
        return device.getId();
    }

    @Override
    public void destroy() {
        if (scriptEngine != null) {
            scriptEngine.destroy();
            scriptEngine = null;
        }
    }

}
