package com.sonian.elasticsearch;

import com.sonian.elasticsearch.equilibrium.ClusterEqualizerService;
import com.sonian.elasticsearch.equilibrium.DiskShardsAllocator;
import com.sonian.elasticsearch.equilibrium.NodeInfoHelper;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author dakrone
 */
public class DiskShardAllocatorIntegrationTests extends AbstractEquilibriumTests {

    @AfterTest
    public void cleanUp() {
        closeAllNodes();
    }


    @Test void injectedDiskShardAllocator() {
        startNode("1");
        ShardsAllocator sa = instance("1", ShardsAllocator.class);
        assertThat("DiskShardsAllocator was injected", sa instanceof DiskShardsAllocator);
    }

    @Test
    public void integrationTestNodeHelperTimeout() {
        startNode("1");
        createIndex("1", "itnht1", 10, 0);
        waitForGreen("1","itnht1","10s");
        TransportIndicesStatsAction tisa = instance("1", TransportIndicesStatsAction.class);
        TransportNodesStatsAction tnsa = instance("1", TransportNodesStatsAction.class);
        Settings s = ImmutableSettings.settingsBuilder()
                     .put("sonian.elasticsearch.equilibrium.shardStatsTimeout", 0, TimeUnit.SECONDS)
                     .put("sonian.elasticsearch.equilibrium.nodeFsStatsTimeout", 0, TimeUnit.SECONDS)
                     .build();
        NodeInfoHelper nih = new NodeInfoHelper(s, tisa, tnsa);
        assertThat("timeout results in a null result", null == nih.nodeFsStats());
        assertThat("timeout results in a null result", null == nih.nodeShardStats());
        deleteIndex("1", "itnht1");
        waitForGreen("1", null, "10s");
    }


    @Test
    public void integrationTestNodeFsStats() {
        startNode("1");
        NodeInfoHelper helper = instance("1", NodeInfoHelper.class);
        DiskShardsAllocator dsa = new DiskShardsAllocator(ImmutableSettings.settingsBuilder().build(), helper);
        NodesStatsResponse resp = helper.nodeFsStats();

        assertThat("averagePercentageFree is always between 0 and 100 percent",
                dsa.averagePercentageFree(resp.getNodes()[0].fs()) < 100.0 &&
                        dsa.averagePercentageFree(resp.getNodes()[0].fs()) > 0.0);

        assertThat("averageAvailableBytes is above 100 bytes",
                dsa.averageAvailableBytes(resp.getNodes()[0].fs()) > 100.0);
    }


    @Test
    public void integrationTestNodeShardStats() {
        startNode("1");

        createIndex("1", "itnss1", 2, 0);
        createIndex("1", "itnss2", 3, 0);
        waitForGreen("1","itnss1","10s");
        waitForGreen("1", "itnss2", "10s");

        NodeInfoHelper helper = instance("1", NodeInfoHelper.class);
        HashMap<ShardId, Long> shardSizes = helper.nodeShardStats();

        assertThat("there are sizes for all shards", shardSizes.size() == 5);
        Iterator<Long> i = shardSizes.values().iterator();
        while (i.hasNext()) {
            Long size = i.next();
            assertThat("each shard has a positive size", size > 0.0);
        }

        deleteIndex("1", "itnss1");
        deleteIndex("1", "itnss2");
        waitForGreen("1", null, "10s");
    }


    @Test
    public void integrationTestEligibleForSwap() {
        startNode("1");

        DiskShardsAllocator dsa = instance("1", DiskShardsAllocator.class);
        NodeInfoHelper nih = instance("1", NodeInfoHelper.class);

        DiscoveryNodes dns = createMock(DiscoveryNodes.class);
        expect(dns.size()).andStubReturn(1);

        RoutingAllocation allocation = createMock(RoutingAllocation.class);
        expect(allocation.nodes()).andStubReturn(dns);
        replay(allocation, dns);

        assertThat("We can't swap with 1 node",
                dsa.eligibleForSwap(allocation, nih.nodeFsStats()) == false);
        assertThat("We can't swap with null NodeStats",
                dsa.eligibleForSwap(allocation, null) == false);

        DiscoveryNodes dns2 = createMock(DiscoveryNodes.class);
        expect(dns2.size()).andStubReturn(2);

        RoutingAllocation allocation2 = createMock(RoutingAllocation.class);
        expect(allocation2.nodes()).andStubReturn(dns2);
        replay(allocation2, dns2);

        assertThat("We can swap with 2 nodes",
                dsa.eligibleForSwap(allocation2, nih.nodeFsStats()));

    }


    // These tests are commented out because I might revisit them as
    // integration tests one day

    //@Test
    public void testEnoughDiskForShard() {
        startNode("1");

        // Mock out disk usage so it returns 50% free

        // create index
        createIndex("1", "i1", 2, 0);

        // assert that cluster is green and shards are assigned
        assertThat("cluster is green", isGreen("1"));

        // Mock out disk usage so it returns 10% free

        // create index
        createIndex("1", "i2", 2, 0);

        // assert that cluster is red and shards are unassigned
        assertThat("cluster is red", isRed("1"));

        deleteIndex("1", "i1");
        deleteIndex("1", "i2");
        waitForGreen("1", null, "10s");
    }

    //@Test
    public void rebalanceTest() {
        startNode("1");
        startNode("2");
        startNode("3");

        // Mock out disk usage so it returns > 20% free

        // create three shards, one shard should go to each machine
        createIndex("1", "i1", 1, 0);
        createIndex("2", "i2", 1, 0);
        createIndex("3", "i3", 1, 0);

        // mock i1's shard so it's large 100mb
        // mock i2's shard so it's medium 50mb
        // mock i3's shard so it's small 1mb

        // kick off rebalancing
        ClusterEqualizerService ce = instance("1", ClusterEqualizerService.class);
        assertThat("kick off rebalancing", ce.equalize());

        // assert that the cluster is green immediately after a rebalance
        assertThat("cluster is green", isGreen("1"));

        // wait for rebalancing to finish

        // assert that the location of i1's shard i3's shard have been swapped

        // assert that the cluster is still green
        assertThat("cluster is green", isGreen("1"));

        deleteIndex("1", "i1");
        deleteIndex("1", "i2");
        deleteIndex("1", "i3");
        waitForGreen("1", null, "10s");
    }
}
