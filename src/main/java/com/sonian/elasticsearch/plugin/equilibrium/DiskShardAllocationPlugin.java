/*
 * Copyright 2011 Sonian Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.sonian.elasticsearch.plugin.equilibrium;

import com.sonian.elasticsearch.equilibrium.DiskShardsAllocatorModule;
import com.sonian.elasticsearch.rest.action.equilibrium.RestNodesEqualizeAction;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import java.util.Collection;

/**
 * @author dakrone
 */
public class DiskShardAllocationPlugin extends AbstractPlugin {

    private final Settings settings;

    public DiskShardAllocationPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override public String name() {
        return "equilibrium";
    }

    @Override public String description() {
        return "Equilibrium Plugin Version: " + Version.number() + " (" + Version.date() + ")";
    }

    @Override public Settings additionalSettings() {
        return super.additionalSettings();
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        return ImmutableList.<Class<? extends Module>>of(DiskShardsAllocatorModule.class);
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof RestModule) {
            ((RestModule) module).addRestAction(RestNodesEqualizeAction.class);
        }
    }
}
