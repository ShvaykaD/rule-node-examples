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

import lombok.Data;
import org.thingsboard.rule.engine.api.NodeConfiguration;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.script.ScriptLanguage;

@Data
public class TbSplitToQueueNodeConfiguration implements NodeConfiguration<TbSplitToQueueNodeConfiguration> {

    private ScriptLanguage scriptLang;
    private String jsScript;
    private String tbelScript;

    /** Entity type the produced messages are re-originated to. */
    private EntityType originatorType;

    /**
     * Name pattern resolved against each produced message to find its new originator, e.g.
     * {@code ${entityName}} where the script puts the name in that message's metadata.
     */
    private String originatorNamePattern;

    @Override
    public TbSplitToQueueNodeConfiguration defaultConfiguration() {
        var configuration = new TbSplitToQueueNodeConfiguration();
        configuration.setScriptLang(ScriptLanguage.TBEL);
        configuration.setJsScript("return [{msg: msg, metadata: metadata, msgType: msgType}];");
        configuration.setTbelScript("return [{msg: msg, metadata: metadata, msgType: msgType}];");
        configuration.setOriginatorType(EntityType.DEVICE);
        configuration.setOriginatorNamePattern("${entityName}");
        return configuration;
    }

}
