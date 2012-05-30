package com.sonian.elasticsearch;

import com.sonian.elasticsearch.equilibrium.ClusterEqualizerService;
import com.sonian.elasticsearch.equilibrium.DiskShardsAllocator;
import com.sonian.elasticsearch.equilibrium.NodeInfoHelper;
import com.sonian.elasticsearch.tests.AbstractJettyHttpServerTests;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author dakrone
 */
public class DiskShardsAllocatorTests extends AbstractJettyHttpServerTests {

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

    @Test void injectedDiskShardAllocator() {
        startNode("1");
        ShardsAllocator sa = instance("1", ShardsAllocator.class);
        assertThat("DiskShardsAllocator was injected", sa instanceof DiskShardsAllocator);
        closeNode("1");
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

        closeNode("1");
    }

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

        closeNode("1");
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

        closeAllNodes();
    }
}
