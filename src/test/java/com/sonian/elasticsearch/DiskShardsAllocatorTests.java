package com.sonian.elasticsearch;

import com.sonian.elasticsearch.equilibrium.DiskShardsAllocator;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.monitor.fs.FsStats;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.easymock.EasyMock.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author dakrone
 */
public class DiskShardsAllocatorTests extends AbstractEquilibriumTests {

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

}
