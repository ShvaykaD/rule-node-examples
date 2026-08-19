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

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.rule.engine.api.ScriptEngine;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.msg.TbNodeConnectionType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.dao.device.DeviceService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.thingsboard.server.common.data.msg.TbMsgType.POST_TELEMETRY_REQUEST;

@ExtendWith(MockitoExtension.class)
public class TbSplitToQueueNodeTest {

    private static final String TARGET_QUEUE = "TargetQueue";
    private static final TenantId TENANT_ID = TenantId.fromUUID(UUID.randomUUID());

    private TbSplitToQueueNode node;

    @Mock
    private TbContext ctxMock;
    @Mock
    private ScriptEngine scriptEngineMock;
    @Mock
    private DeviceService deviceServiceMock;

    @BeforeEach
    public void setUp() {
        node = new TbSplitToQueueNode();
        lenient().when(ctxMock.getQueueName()).thenReturn(TARGET_QUEUE);
        lenient().when(ctxMock.getTenantId()).thenReturn(TENANT_ID);
        lenient().when(ctxMock.getDeviceService()).thenReturn(deviceServiceMock);
        lenient().when(ctxMock.createScriptEngine(any(), anyString())).thenReturn(scriptEngineMock);
        lenient().when(deviceServiceMock.findDeviceByTenantIdAndName(eq(TENANT_ID), anyString()))
                .thenAnswer(invocation -> deviceNamed(invocation.getArgument(1)));
    }

    // ---------- init ----------

    @Test
    public void givenDefaultConfig_whenInit_thenTargetQueueIsTakenFromTheNode() throws TbNodeException {
        initWithDefaultConfig();

        then(ctxMock).should().getQueueName();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"  "})
    public void givenBlankOriginatorNamePattern_whenInit_thenThrowsException(String pattern) {
        var config = new TbSplitToQueueNodeConfiguration().defaultConfiguration();
        config.setOriginatorNamePattern(pattern);

        assertThatThrownBy(() -> node.init(ctxMock, configurationOf(config)))
                .isInstanceOf(TbNodeException.class)
                .hasMessageContaining("Originator name pattern is not set");
    }

    @Test
    public void givenNoOriginatorType_whenInit_thenThrowsException() {
        var config = new TbSplitToQueueNodeConfiguration().defaultConfiguration();
        config.setOriginatorType(null);

        assertThatThrownBy(() -> node.init(ctxMock, configurationOf(config)))
                .isInstanceOf(TbNodeException.class)
                .hasMessageContaining("Originator type is not set");
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void givenBlankScript_whenInit_thenThrowsException(String script) {
        var config = new TbSplitToQueueNodeConfiguration().defaultConfiguration();
        config.setTbelScript(script);

        assertThatThrownBy(() -> node.init(ctxMock, configurationOf(config)))
                .isInstanceOf(TbNodeException.class)
                .hasMessageContaining("Script is not set");
    }

    // ---------- the behaviour that distinguishes this node ----------

    @Test
    public void givenScriptProducesManyMessages_whenOnMsg_thenEachGoesToTheConfiguredQueue() throws TbNodeException {
        initWithDefaultConfig();
        var incoming = msgWithEntityName("ignored");
        given(scriptEngineMock.executeUpdateAsync(incoming)).willReturn(Futures.immediateFuture(
                List.of(msgWithEntityName("device-1"), msgWithEntityName("device-2"), msgWithEntityName("device-3"))));

        node.onMsg(ctxMock, incoming);

        var queueCaptor = ArgumentCaptor.forClass(String.class);
        then(ctxMock).should(times(3)).enqueueForTellNext(
                any(TbMsg.class), queueCaptor.capture(), eq(TbNodeConnectionType.SUCCESS), any(), any());
        assertThat(queueCaptor.getAllValues()).containsOnly(TARGET_QUEUE);
    }

    @Test
    public void givenScriptProducesManyMessages_whenOnMsg_thenEachCarriesItsOwnResolvedOriginator() throws TbNodeException {
        initWithDefaultConfig();
        var incoming = msgWithEntityName("ignored");
        given(scriptEngineMock.executeUpdateAsync(incoming)).willReturn(Futures.immediateFuture(
                List.of(msgWithEntityName("device-1"), msgWithEntityName("device-2"), msgWithEntityName("device-3"))));

        node.onMsg(ctxMock, incoming);

        var msgCaptor = ArgumentCaptor.forClass(TbMsg.class);
        then(ctxMock).should(times(3)).enqueueForTellNext(
                msgCaptor.capture(), anyString(), anyString(), any(), any());
        assertThat(msgCaptor.getAllValues())
                .extracting(TbMsg::getOriginator)
                .allMatch(originator -> EntityType.DEVICE.equals(originator.getEntityType()))
                .doesNotHaveDuplicates()
                .doesNotContain((EntityId) incoming.getOriginator());
    }

    // ---------- forwarding contract ----------

    @Test
    public void givenAllEnqueuesSucceed_whenOnMsg_thenIncomingGoesToSuccessOnceAllAreDone() throws TbNodeException {
        initWithDefaultConfig();
        var incoming = msgWithEntityName("ignored");
        given(scriptEngineMock.executeUpdateAsync(incoming)).willReturn(Futures.immediateFuture(
                List.of(msgWithEntityName("device-1"), msgWithEntityName("device-2"))));
        var callbacks = captureEnqueueCallbacks();

        node.onMsg(ctxMock, incoming);

        callbacks.onSuccess.get(0).run();
        then(ctxMock).should(never()).tellSuccess(incoming);

        callbacks.onSuccess.get(1).run();
        then(ctxMock).should(times(1)).tellSuccess(incoming);
    }

    @Test
    public void givenOneEnqueueFails_whenOnMsg_thenIncomingIsFailedAndNeverForwarded() throws TbNodeException {
        initWithDefaultConfig();
        var incoming = msgWithEntityName("ignored");
        given(scriptEngineMock.executeUpdateAsync(incoming)).willReturn(Futures.immediateFuture(
                List.of(msgWithEntityName("device-1"), msgWithEntityName("device-2"))));
        var callbacks = captureEnqueueCallbacks();
        var error = new RuntimeException("enqueue failed");

        node.onMsg(ctxMock, incoming);
        callbacks.onFailure.get(0).accept(error);
        callbacks.onSuccess.get(1).run();

        then(ctxMock).should().tellFailure(incoming, error);
        then(ctxMock).should(never()).tellSuccess(incoming);
    }

    @Test
    public void givenTwoEnqueuesFail_whenOnMsg_thenIncomingIsFailedOnlyOnce() throws TbNodeException {
        initWithDefaultConfig();
        var incoming = msgWithEntityName("ignored");
        given(scriptEngineMock.executeUpdateAsync(incoming)).willReturn(Futures.immediateFuture(
                List.of(msgWithEntityName("device-1"), msgWithEntityName("device-2"))));
        var callbacks = captureEnqueueCallbacks();

        node.onMsg(ctxMock, incoming);
        callbacks.onFailure.get(0).accept(new RuntimeException("first"));
        callbacks.onFailure.get(1).accept(new RuntimeException("second"));

        then(ctxMock).should(times(1)).tellFailure(eq(incoming), any(Throwable.class));
    }

    // ---------- failure paths ----------

    @Test
    public void givenAnUnresolvableEntityName_whenOnMsg_thenNothingIsEnqueued() throws TbNodeException {
        initWithDefaultConfig();
        given(deviceServiceMock.findDeviceByTenantIdAndName(TENANT_ID, "missing")).willReturn(null);
        var incoming = msgWithEntityName("ignored");
        given(scriptEngineMock.executeUpdateAsync(incoming)).willReturn(Futures.immediateFuture(
                List.of(msgWithEntityName("device-1"), msgWithEntityName("missing"))));

        node.onMsg(ctxMock, incoming);

        // resolution happens for the whole batch before any enqueue, so a partial fan-out cannot be committed
        then(ctxMock).should(never()).enqueueForTellNext(any(TbMsg.class), anyString(), anyString(), any(), any());
        then(ctxMock).should().tellFailure(eq(incoming), any(Throwable.class));
        then(ctxMock).should(never()).tellSuccess(incoming);
    }

    @Test
    public void givenScriptProducesNoMessages_whenOnMsg_thenIncomingIsFailed() throws TbNodeException {
        initWithDefaultConfig();
        var incoming = msgWithEntityName("ignored");
        given(scriptEngineMock.executeUpdateAsync(incoming)).willReturn(Futures.immediateFuture(List.of()));

        node.onMsg(ctxMock, incoming);

        then(ctxMock).should(never()).enqueueForTellNext(any(TbMsg.class), anyString(), anyString(), any(), any());
        then(ctxMock).should().tellFailure(eq(incoming), any(Throwable.class));
    }

    @Test
    public void givenScriptFails_whenOnMsg_thenIncomingIsFailed() throws TbNodeException {
        initWithDefaultConfig();
        var incoming = msgWithEntityName("ignored");
        var error = new RuntimeException("script blew up");
        given(scriptEngineMock.executeUpdateAsync(incoming)).willReturn(Futures.immediateFailedFuture(error));

        node.onMsg(ctxMock, incoming);

        then(ctxMock).should().tellFailure(incoming, error);
        then(ctxMock).should(never()).enqueueForTellNext(any(TbMsg.class), anyString(), anyString(), any(), any());
    }

    // ---------- lifecycle ----------

    @Test
    public void givenInitialisedNode_whenDestroy_thenScriptEngineIsReleased() throws TbNodeException {
        initWithDefaultConfig();

        node.destroy();

        then(scriptEngineMock).should().destroy();
    }

    // ---------- helpers ----------

    private void initWithDefaultConfig() throws TbNodeException {
        node.init(ctxMock, configurationOf(new TbSplitToQueueNodeConfiguration().defaultConfiguration()));
    }

    private static TbNodeConfiguration configurationOf(TbSplitToQueueNodeConfiguration config) {
        return new TbNodeConfiguration(JacksonUtil.valueToTree(config));
    }

    private static Device deviceNamed(String name) {
        var device = new Device(new DeviceId(UUID.randomUUID()));
        device.setName(name);
        return device;
    }

    private static TbMsg msgWithEntityName(String name) {
        return TbMsg.newMsg()
                .type(POST_TELEMETRY_REQUEST)
                .originator(new DeviceId(UUID.randomUUID()))
                .metaData(new TbMsgMetaData(Map.of("entityName", name)))
                .data("{}")
                .build();
    }

    /** Captures the per-message enqueue callbacks so the forwarding contract can be driven explicitly. */
    private EnqueueCallbacks captureEnqueueCallbacks() {
        var captured = new EnqueueCallbacks();
        willAnswer(invocation -> {
            captured.onSuccess.add(invocation.getArgument(3));
            captured.onFailure.add(invocation.getArgument(4));
            return null;
        }).given(ctxMock).enqueueForTellNext(any(TbMsg.class), anyString(), anyString(), any(), any());
        return captured;
    }

    private static final class EnqueueCallbacks {
        final List<Runnable> onSuccess = new ArrayList<>();
        final List<Consumer<Throwable>> onFailure = new ArrayList<>();
    }

}
