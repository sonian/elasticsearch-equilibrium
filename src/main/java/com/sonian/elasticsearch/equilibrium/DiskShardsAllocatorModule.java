package com.sonian.elasticsearch.equilibrium;

import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocatorModule;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.settings.Settings;

/**
 * @author dakrone
 */
public class DiskShardsAllocatorModule extends AbstractModule implements PreProcessModule {

    private final Settings componentSettings;

    public DiskShardsAllocatorModule(Settings settings) {
        this.componentSettings = settings.getComponentSettings(this.getClass());
    }

    @Override
    protected void configure() {
        bind(ClusterEqualizerService.class).asEagerSingleton();
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof ShardsAllocatorModule &&
                this.componentSettings.getAsBoolean("enabled", false)) {
            ((ShardsAllocatorModule) module).setShardsAllocator(DiskShardsAllocator.class);
        }
    }
}
