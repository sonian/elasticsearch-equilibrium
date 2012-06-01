package com.sonian.elasticsearch;

import com.sonian.elasticsearch.equilibrium.ClusterEqualizerService;
import com.sonian.elasticsearch.equilibrium.NodeInfoHelper;
import com.sonian.elasticsearch.tests.AbstractJettyHttpServerTests;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author dakrone
 */
public class DiskShardAllocatorIntegrationTests extends AbstractJettyHttpServerTests {
    TestUtils tu = new TestUtils();

    @AfterTest
    public void cleanUp() {
        tu.closeAllNodes();
    }


    @Test
    public void integrationTestNodeHelperTimeout() {
        tu.startNode("1");
        tu.createIndex("1","i1",10, 0);
        TransportIndicesStatsAction tisa = tu.instance("1", TransportIndicesStatsAction.class);
        TransportNodesStatsAction tnsa = tu.instance("1", TransportNodesStatsAction.class);
        Settings s = ImmutableSettings.settingsBuilder()
                     .put("sonian.elasticsearch.equilibrium.shardStatsTimeout", 0, TimeUnit.SECONDS)
                     .put("sonian.elasticsearch.equilibrium.nodeFsStatsTimeout", 0, TimeUnit.SECONDS)
                     .build();
        NodeInfoHelper nih = new NodeInfoHelper(s, tisa, tnsa);
        assertThat("timeout results in a null result", null == nih.nodeFsStats());
        assertThat("timeout results in a null result", null == nih.nodeShardStats());
        tu.deleteIndex("1", "i1");
    }

    // These tests are commented out because I might revisit them as
    // integration tests one day

    //@Test
    public void testEnoughDiskForShard() {
        tu.startNode("1");

        // Mock out disk usage so it returns 50% free

        // create index
        tu.createIndex("1", "i1", 2, 0);

        // assert that cluster is green and shards are assigned
        assertThat("cluster is green", tu.isGreen("1"));

        // Mock out disk usage so it returns 10% free

        // create index
        tu.createIndex("1", "i2", 2, 0);

        // assert that cluster is red and shards are unassigned
        assertThat("cluster is red", tu.isRed("1"));

        tu.deleteIndex("1", "i1");
        tu.deleteIndex("1", "i2");
    }

    //@Test
    public void rebalanceTest() {
        tu.startNode("1");
        tu.startNode("2");
        tu.startNode("3");

        // Mock out disk usage so it returns > 20% free

        // create three shards, one shard should go to each machine
        tu.createIndex("1", "i1", 1, 0);
        tu.createIndex("2", "i2", 1, 0);
        tu.createIndex("3", "i3", 1, 0);

        // mock i1's shard so it's large 100mb
        // mock i2's shard so it's medium 50mb
        // mock i3's shard so it's small 1mb

        // kick off rebalancing
        ClusterEqualizerService ce = instance("1", ClusterEqualizerService.class);
        assertThat("kick off rebalancing", ce.equalize());

        // assert that the cluster is green immediately after a rebalance
        assertThat("cluster is green", tu.isGreen("1"));

        // wait for rebalancing to finish

        // assert that the location of i1's shard i3's shard have been swapped

        // assert that the cluster is still green
        assertThat("cluster is green", tu.isGreen("1"));

        tu.deleteIndex("1", "i1");
        tu.deleteIndex("1", "i2");
        tu.deleteIndex("1", "i3");
    }
}
