package com.sonian.elasticsearch.equilibrium;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;

/**
 * @author dakrone
 */
public class NodeInfoHelper extends AbstractComponent {

    // action used to gather FsStats for all the nodes in the cluster, to check
    // their disk usage
    private final TransportNodesStatsAction nodesStatsAction;

    // action used to gather shard sizes for all nodes in the cluster
    private final TransportIndicesStatsAction indicesStatsAction;

    @Inject
    public NodeInfoHelper(Settings settings, TransportIndicesStatsAction indicesStatsAction,
                          TransportNodesStatsAction nodesStatsAction) {
        super(settings);
        this.indicesStatsAction = indicesStatsAction;
        this.nodesStatsAction = nodesStatsAction;
    }



    /**
     * Return the FS stats for all nodes, times out if no responses are
     * returned in 10 minutes, returning null if timed out
     *
     * @return NodesStatsResponse for the FsStats for the cluster
     */
    public NodesStatsResponse nodeFsStats() {
        logger.trace("nodeFsStats");
        NodesStatsResponse resp;
        try {
            NodesStatsRequest request = new NodesStatsRequest(Strings.EMPTY_ARRAY);
            request.timeout(TimeValue.timeValueMillis(10 * 60 * 1000)); // 10 minutes
            request.clear();
            request.fs(true);
            resp = nodesStatsAction.execute(request).actionGet(20000);
        } catch (Exception e) {
            logger.error("Exception getting nodeFsStats for all nodes.", e);
            return null;
        }
        return resp;
    }


    /**
     * Retrieves the shard sizes for all shards in the cluster, waits 15
     * seconds for a response from the cluster nodes
     *
     * @return a Map of ShardId to size in bytes of the shard
     */
    public HashMap<ShardId, Long> nodeShardStats() {
        logger.trace("nodeShardStats");
        final HashMap<ShardId, Long> shardSizes = new HashMap<ShardId, Long>();
        IndicesStatsRequest request = new IndicesStatsRequest();
        request.clear();
        request.store(true);
        IndicesStats resp;
        try {
            resp = indicesStatsAction.execute(request).actionGet(15000);
        } catch (Exception e) {
            logger.error("Exception getting shard stats for each node.", e);
            return null;
        }
        for (ShardStats stats : resp.getShards()) {
            shardSizes.put(stats.getShardRouting().shardId(), stats.stats().store().getSizeInBytes());
        }
        return shardSizes;
    }


}
