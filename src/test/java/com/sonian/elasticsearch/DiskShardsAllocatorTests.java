package com.sonian.elasticsearch;

import com.sonian.elasticsearch.equilibrium.ClusterEqualizerService;
import com.sonian.elasticsearch.equilibrium.DiskShardsAllocator;
import com.sonian.elasticsearch.equilibrium.NodeInfoHelper;
import com.sonian.elasticsearch.tests.AbstractJettyHttpServerTests;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
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


    // Helpers for tests
    public void createIndex(String id, String name, int numberOfShards, int numberOfRelicas) {
        Client c = client(id);
        c.admin().indices().prepareCreate(name)
                 .setSettings(ImmutableSettings.settingsBuilder()
                              .put("number_of_shards", numberOfShards)
                              .put("number_of_replicas", numberOfRelicas))
                 .execute().actionGet();
    }

    protected void deleteIndex(String id, String name) {
        try {
            client(id).admin().indices().prepareDelete(name).execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
    }

    public ClusterHealthStatus getStatus(String id) {
        Client c = client(id);
        ClusterHealthResponse healthResponse = c.admin().cluster().prepareHealth().setTimeout("2s").execute().actionGet();
        return healthResponse.status();
    }

    public boolean isGreen (String id) {
        return ClusterHealthStatus.GREEN == getStatus(id);
    }

    public boolean isYellow (String id) {
        return ClusterHealthStatus.YELLOW == getStatus(id);
    }

    public boolean isRed (String id) {
        return ClusterHealthStatus.RED == getStatus(id);
    }

    public FsStats makeFakeFsStats(long total, long avail) {
        FsStats fs = createMock(FsStats.class);
        FsStats.Info[] infos = new FsStats.Info[1];

        FsStats.Info fsInfo1 = createMock(FsStats.Info.class);
        expect(fsInfo1.total()).andStubReturn(new ByteSizeValue(total));
        expect(fsInfo1.available()).andStubReturn(new ByteSizeValue(avail));

        infos[0] = fsInfo1;
        expect(fs.iterator()).andStubReturn(Iterators.forArray(infos));

        replay(fs, fsInfo1);

        return fs;
    }


    // Testing functions
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
    public void unitTestNodeFsStats() {
        startNode("1");
        NodeInfoHelper helper = instance("1", NodeInfoHelper.class);
        DiskShardsAllocator dsa = instance("1", DiskShardsAllocator.class);
        NodesStatsResponse resp = helper.nodeFsStats();

        assertThat("averagePercentageFree is always between 0 and 100 percent",
                   dsa.averagePercentageFree(resp.getNodes()[0].fs()) < 100.0 &&
                   dsa.averagePercentageFree(resp.getNodes()[0].fs()) > 0.0);

        assertThat("averageAvailableBytes is above 100 bytes",
                dsa.averageAvailableBytes(resp.getNodes()[0].fs()) > 100.0);
    }

    @Test
    public void unitTestNodeShardStats() {
        startNode("1");

        createIndex("1", "i1", 2, 0);
        createIndex("1", "i2", 3, 0);

        NodeInfoHelper helper = instance("1", NodeInfoHelper.class);
        HashMap<ShardId, Long> shardSizes = helper.nodeShardStats();

        assertThat("there are sizes for all shards", shardSizes.size() == 5);
        Iterator<Long> i = shardSizes.values().iterator();
        while (i.hasNext()) {
            Long size = i.next();
            assertThat("each shard has a positive size", size > 0.0);
        }

        deleteIndex("1", "i1");
        deleteIndex("1", "i2");
    }

    @Test
    public void unitTestEnoughDiskForShard() {
        startNode("1");

        DiskShardsAllocator dsa = instance("1", DiskShardsAllocator.class);

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
    }
}
