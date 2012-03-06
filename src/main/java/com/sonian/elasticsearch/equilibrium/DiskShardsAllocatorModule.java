package com.sonian.elasticsearch.equilibrium;

import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocatorModule;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;

/**
 * @author dakrone
 */
public class DiskShardsAllocatorModule extends AbstractModule implements PreProcessModule {

    @Override
    protected void configure() {
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof ShardsAllocatorModule) {
            ((ShardsAllocatorModule) module).setShardsAllocator(DiskShardsAllocator.class);
        }
    }
}
