package com.sonian.elasticsearch;

import com.sonian.elasticsearch.equilibrium.ClusterEqualizerService;
import com.sonian.elasticsearch.equilibrium.DiskShardsAllocator;
import com.sonian.elasticsearch.equilibrium.NodeInfoHelper;
import com.sonian.elasticsearch.tests.AbstractJettyHttpServerTests;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.fs.FsStats;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;

import static org.easymock.EasyMock.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author dakrone
 */
public class DiskShardsAllocatorTests extends AbstractJettyHttpServerTests {
    TestUtils tu = new TestUtils();

    // Testing functions
    @AfterTest
    public void cleanUp() {
        closeAllNodes();
    }

    @Test void injectedDiskShardAllocator() {
        tu.startNode("1");
        ShardsAllocator sa = tu.instance("1", ShardsAllocator.class);
        assertThat("DiskShardsAllocator was injected", sa instanceof DiskShardsAllocator);
    }

    @Test
    public void unitTestNodeFsStats() {
        NodeInfoHelper helper = tu.instance("1", NodeInfoHelper.class);
        DiskShardsAllocator dsa = new DiskShardsAllocator(ImmutableSettings.settingsBuilder().build(), helper);
        NodesStatsResponse resp = helper.nodeFsStats(10000);

        assertThat("averagePercentageFree is always between 0 and 100 percent",
                   dsa.averagePercentageFree(resp.getNodes()[0].fs()) < 100.0 &&
                   dsa.averagePercentageFree(resp.getNodes()[0].fs()) > 0.0);

        assertThat("averageAvailableBytes is above 100 bytes",
                dsa.averageAvailableBytes(resp.getNodes()[0].fs()) > 100.0);
    }

    @Test
    public void unitTestNodeShardStats() {
        tu.startNode("1");

        tu.createIndex("1", "i1", 2, 0);
        tu.createIndex("1", "i2", 3, 0);

        NodeInfoHelper helper = tu.instance("1", NodeInfoHelper.class);
        HashMap<ShardId, Long> shardSizes = helper.nodeShardStats(10000);

        assertThat("there are sizes for all shards", shardSizes.size() == 5);
        Iterator<Long> i = shardSizes.values().iterator();
        while (i.hasNext()) {
            Long size = i.next();
            assertThat("each shard has a positive size", size > 0.0);
        }

        tu.deleteIndex("1", "i1");
        tu.deleteIndex("1", "i2");
    }

    @Test
    public void unitTestEnoughDiskForShard() {
        DiskShardsAllocator dsa = new DiskShardsAllocator(ImmutableSettings.settingsBuilder().build(), null);

        MutableShardRouting msr = new MutableShardRouting("i1", 0, "node1", true,
                                                          ShardRoutingState.UNASSIGNED, 0);

        DiscoveryNode dn = new DiscoveryNode("node1", "node1", null,
                                             new HashMap<String, String>());
        RoutingNode node = new RoutingNode("node1", dn);

        FsStats fakeSmallFs = tu.makeFakeFsStats(1000, 10);
        FsStats fakeLargeFs = tu.makeFakeFsStats(1000, 999);

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

}
