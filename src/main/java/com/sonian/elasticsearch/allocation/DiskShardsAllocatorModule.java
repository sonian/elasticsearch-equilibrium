package com.sonian.elasticsearch.allocation;

import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.inject.AbstractModule;

/**
 * @author dakrone
 */
public class DiskShardsAllocatorModule extends AbstractModule {
    
    @Override
    protected void configure() {
        bind(ShardsAllocator.class).to(DiskShardsAllocator.class).asEagerSingleton();
    }
}
