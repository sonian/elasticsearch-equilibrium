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
     * Return the FS stats for all nodes, returning null if timed out.
     * Defaults to a timeout of 10 minutes if timeout parameter is null.
     *
     * @return NodesStatsResponse for the FsStats for the cluster
     */
    public NodesStatsResponse nodeFsStats(Long maybeTimeout) {
        logger.trace("nodeFsStats");
        NodesStatsResponse resp;

        long timeout = (maybeTimeout == null) ? maybeTimeout : (10 * 60 * 1000);

        try {
            NodesStatsRequest request = new NodesStatsRequest(Strings.EMPTY_ARRAY);
            request.timeout(TimeValue.timeValueMillis(timeout));
            request.clear();
            request.fs(true);
            resp = nodesStatsAction.execute(request).actionGet(timeout);
        } catch (Exception e) {
            logger.error("Exception getting nodeFsStats for all nodes.", e);
            return null;
        }
        return resp;
    }


    /**
     * Retrieves the shard sizes for all shards in the cluster, returns null
     * if an exception occurs. Defaults to a timeout of 15 seconds if the
     * timeout parameter is not set
     *
     * @return a Map of ShardId to size in bytes of the shard
     */
    public HashMap<ShardId, Long> nodeShardStats(Long maybeTimeout) {
        logger.trace("nodeShardStats");
        final HashMap<ShardId, Long> shardSizes = new HashMap<ShardId, Long>();
        IndicesStatsRequest request = new IndicesStatsRequest();
        request.clear();
        request.store(true);
        IndicesStats resp;

        long timeout = (maybeTimeout == null) ? maybeTimeout : 15000;

        try {
            resp = indicesStatsAction.execute(request).actionGet(timeout);
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
