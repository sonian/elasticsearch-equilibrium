package com.sonian.elasticsearch;

import com.sonian.elasticsearch.equilibrium.DiskShardsAllocator;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.fs.FsStats;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.easymock.EasyMock.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author dakrone
 */
public class DiskShardsAllocatorUnitTests extends AbstractEquilibriumTests {

    @Test
    public void unitTestEnoughDiskForShard() {
        DiskShardsAllocator dsa = new DiskShardsAllocator(ImmutableSettings.settingsBuilder().build(), null);

        MutableShardRouting msr = new MutableShardRouting("i1", 0, "node1", true,
                                                          ShardRoutingState.UNASSIGNED, 0);

        DiscoveryNode dn = new DiscoveryNode("node1", "node1", null,
                                             new HashMap<String, String>());
        RoutingNode node = new RoutingNode("node1", dn);

        FsStats fakeSmallFs = makeFakeFsStats(1000, 10);
        FsStats fakeLargeFs = makeFakeFsStats(1000, 999);

        HashMap<String, NodeStats> fakeSmallStats = new HashMap<String, NodeStats>();
        fakeSmallStats.put("node1", new NodeStats(dn, 0, "hostname", null,
                           null, null, null, null, null, fakeSmallFs, null, null));

        HashMap<String, NodeStats> fakeLargeStats = new HashMap<String, NodeStats>();
        fakeLargeStats.put("node1", new NodeStats(dn, 0, "hostname", null,
                           null, null, null, null, null, fakeLargeFs, null, null));

        NodesStatsResponse smallNSR = createMock(NodesStatsResponse.class);
        expect(smallNSR.getNodesMap()).andStubReturn(fakeSmallStats);
        replay(smallNSR);

        NodesStatsResponse largeNSR = createMock(NodesStatsResponse.class);
        expect(largeNSR.getNodesMap()).andStubReturn(fakeLargeStats);
        replay(largeNSR);

        boolean resp1 = dsa.enoughDiskForShard(msr, node, smallNSR);
        boolean resp2 = dsa.enoughDiskForShard(msr, node, largeNSR);

        assertThat("we don't have enough disk on the small node", resp1 == false);
        assertThat("we do have enough disk on the large node", resp2 == true);

    }

    @Test
    public void unitTestNodesDifferEnoughToSwap() {
        DiskShardsAllocator dsa = new DiskShardsAllocator(ImmutableSettings.settingsBuilder().build(), null);

        DiscoveryNode dn1 = new DiscoveryNode("node1", "node1", null, new HashMap<String, String>());
        DiscoveryNode dn2 = new DiscoveryNode("node2", "node2", null, new HashMap<String, String>());
        RoutingNode node1 = new RoutingNode("node1", dn1);
        RoutingNode node2 = new RoutingNode("node2", dn2);

        FsStats fs1 = makeFakeFsStats(10, 1);
        FsStats fs2 = makeFakeFsStats(10, 9);

        HashMap<String, NodeStats> fakeNodeStats = new HashMap<String, NodeStats>();
        fakeNodeStats.put("node1", new NodeStats(dn1, 0, "hostname", null,
                null, null, null, null, null, fs1, null, null));
        fakeNodeStats.put("node2", new NodeStats(dn2, 0, "hostname", null,
                null, null, null, null, null, fs2, null, null));

        NodesStatsResponse fakeNSR = createMock(NodesStatsResponse.class);
        expect(fakeNSR.getNodesMap()).andStubReturn(fakeNodeStats);
        replay(fakeNSR);

        assertThat("node1 differs enough from node2 to swap",
                dsa.nodesDifferEnoughToSwap(node1, node2, fakeNSR));
        assertThat("node2 is not different enough from node1 to swap",
                !dsa.nodesDifferEnoughToSwap(node2, node1, fakeNSR));
    }

    @Test
    public void unitTestShardsDifferEnoughToSwap() {
        DiskShardsAllocator dsa = new DiskShardsAllocator(ImmutableSettings.settingsBuilder().build(), null);
        MutableShardRouting largeShard = new MutableShardRouting("i1", 0, "node1", true, ShardRoutingState.UNASSIGNED, 0);
        MutableShardRouting smallShard = new MutableShardRouting("i2", 0, "node1", true, ShardRoutingState.UNASSIGNED, 0);
        MutableShardRouting mediumShard = new MutableShardRouting("i3", 0, "node1", true, ShardRoutingState.UNASSIGNED, 0);
        MutableShardRouting zeroShard = new MutableShardRouting("i4", 0, "node1", true, ShardRoutingState.UNASSIGNED, 0);

        HashMap<ShardId, Long> sizes = new HashMap<ShardId, Long>();
        sizes.put(largeShard.shardId(), (long) 100);
        sizes.put(smallShard.shardId(), (long) 10);
        sizes.put(mediumShard.shardId(), (long) 74);
        sizes.put(zeroShard.shardId(), (long) 0);

        assertThat("the shards are different enough to swap",
                dsa.shardsDifferEnoughToSwap(largeShard, smallShard, sizes));
        assertThat("the shards are different enough to swap",
                dsa.shardsDifferEnoughToSwap(largeShard, mediumShard, sizes));
        assertThat("the shards are not different enough to swap",
                !dsa.shardsDifferEnoughToSwap(smallShard, largeShard, sizes));

        assertThat("zero-size shard aborts swap",
                !dsa.shardsDifferEnoughToSwap(largeShard, zeroShard, sizes));
        assertThat("zero-size shard aborts swap",
                !dsa.shardsDifferEnoughToSwap(zeroShard, smallShard, sizes));

        assertThat("null shard aborts swap",
                !dsa.shardsDifferEnoughToSwap(largeShard, null, sizes));
        assertThat("null shard aborts swap",
                !dsa.shardsDifferEnoughToSwap(null, smallShard, sizes));
    }

    @Test
    public void unitTestFirstShardThatCanBeRelocated() {
        DiskShardsAllocator dsa = new DiskShardsAllocator(ImmutableSettings.settingsBuilder().build(), null);

        AllocationDeciders deciders = createMock(AllocationDeciders.class);
        expect(deciders.canAllocate((MutableShardRouting)anyObject(),
                (RoutingNode)anyObject(),
                (RoutingAllocation)anyObject()))
                .andReturn(AllocationDecider.Decision.NO)
                .andReturn(AllocationDecider.Decision.YES)
                .andStubReturn(AllocationDecider.Decision.NO);
        replay(deciders);

        RoutingAllocation allocation = createMock(RoutingAllocation.class);
        expect(allocation.deciders()).andStubReturn(deciders);
        replay(allocation);

        MutableShardRouting s1 = new MutableShardRouting("i1", 0, "node1", true, ShardRoutingState.UNASSIGNED, 0);
        MutableShardRouting s2 = new MutableShardRouting("i2", 0, "node1", true, ShardRoutingState.UNASSIGNED, 0);
        MutableShardRouting s3 = new MutableShardRouting("i3", 0, "node1", true, ShardRoutingState.UNASSIGNED, 0);
        List<MutableShardRouting> shards = new ArrayList<MutableShardRouting>();
        shards.add(s1);
        shards.add(s2);
        shards.add(s3);

        DiscoveryNode dn = new DiscoveryNode("node1", "node1", null, new HashMap<String, String>());
        RoutingNode rn = new RoutingNode("node1", dn);

        assertThat("the first valid shard is returned",
                dsa.firstShardThatCanBeRelocated(allocation, shards, rn) == s2);
    }

}
