package com.sonian.elasticsearch.allocation;

import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.EvenShardsCountAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.logging.Logger;

/**
 * @author dakrone
 */
public class DiskShardsAllocator extends EvenShardsCountAllocator implements ShardsAllocator {

    @Inject
    public DiskShardsAllocator(Settings settings) {
        super(settings);
    }

    @Override
    public void applyStartedShards(StartedRerouteAllocation allocation) {
        logger.info("applyStartedShards");
        super.applyStartedShards(allocation);
    }

    @Override
    public void applyFailedShards(FailedRerouteAllocation allocation) {
        logger.info("applyFailedShards");
        super.applyFailedShards(allocation);
    }

    @Override
    public boolean allocateUnassigned(RoutingAllocation allocation) {
        logger.info("allocateUnassigned");
        return super.allocateUnassigned(allocation);
    }

    @Override
    public boolean rebalance(RoutingAllocation allocation) {
        logger.info("rebalance");
        return super.rebalance(allocation);
    }

    @Override
    public boolean move(MutableShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        logger.info("move");
        return super.move(shardRouting, node, allocation);
    }
}
