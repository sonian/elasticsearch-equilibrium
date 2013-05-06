package com.sonian.elasticsearch.equilibrium;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.fs.FsStats;

import java.util.*;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

/**
 * @author dakrone
 */
public class NodeInfoHelper extends AbstractComponent {

    // action used to gather FsStats for all the nodes in the cluster, to check
    // their disk usage
    private final TransportNodesStatsAction nodesStatsAction;

    // action used to gather shard sizes for all nodes in the cluster
    private final TransportIndicesStatsAction indicesStatsAction;

    // configurable timeouts for shard/node status calls
    private final TimeValue shardStatsTimeout;
    private final TimeValue nodeFsStatsTimeout;

    // configurable timeout for caching node Fs stats
    private final TimeValue nodeFsStatsCacheTTL;

    private NodesStatsResponse cachedNodeStats = null;
    private long lastNodeStatsTime = 0L;

    @Inject
    public NodeInfoHelper(Settings settings, TransportIndicesStatsAction indicesStatsAction,
                          TransportNodesStatsAction nodesStatsAction) {
        super(settings);
        this.indicesStatsAction = indicesStatsAction;
        this.nodesStatsAction = nodesStatsAction;

        Settings compSettings = settings.getComponentSettings(this.getClass());

        // read in timeout values for stats calls, defaulting to 15 seconds
        // for shardStats and 10 minutes for FsStats
        this.shardStatsTimeout = compSettings.getAsTime("shardStatsTimeout",
                                                        TimeValue.timeValueSeconds(15));
        this.nodeFsStatsTimeout = compSettings.getAsTime("nodeFsStatsTimeout",
                                                         TimeValue.timeValueMinutes(10));
        // default to caching nodeFsStats for a minimum of 30 seconds
        this.nodeFsStatsCacheTTL = compSettings.getAsTime("nodeFsStatsCacheTTL",
                                                          TimeValue.timeValueSeconds(30));
        logger.debug("shardStatsTimeout: {}, nodeFsStatsTimeout: {}, nodeFsStatsCacheTTL: {}",
                     this.shardStatsTimeout, this.nodeFsStatsTimeout, this.nodeFsStatsCacheTTL);
    }



    /**
     * Return the FS stats for all nodes, returning null if timed out or an
     * exception is caught.
     *
     * @return NodesStatsResponse for the FsStats for the cluster
     */
    public NodesStatsResponse nodeFsStats() {
        logger.trace("nodeFsStats");
        NodesStatsResponse resp;

        try {
            NodesStatsRequest request = new NodesStatsRequest(Strings.EMPTY_ARRAY);
            request.timeout(nodeFsStatsTimeout);
            request.clear();
            request.fs(true);
            resp = nodesStatsAction.execute(request).actionGet(nodeFsStatsTimeout);
        } catch (Exception e) {
            logger.error("Exception getting nodeFsStats for all nodes.", e);
            return null;
        }
        return resp;
    }


    /**
     */
    public NodesStatsResponse cachedNodeFsStats() {
        logger.trace("cachedNodeFsStats");

        long now = new Date().getTime();
        long cachediff = now - this.lastNodeStatsTime;
        logger.debug("checking cache time difference: {} > {} ?", cachediff, this.nodeFsStatsCacheTTL.millis());

        if (this.cachedNodeStats == null) {
            logger.info("Never retrieved nodeFsStats, retrieving...");
        }
        if (this.cachedNodeStats == null || (cachediff > this.nodeFsStatsCacheTTL.millis())) {
            try {
                logger.info("Executing nodeStatsAction...");
                this.cachedNodeStats = this.nodeFsStats();
                logger.info("finished executing nodeStatsAction.");
                this.lastNodeStatsTime = now;
            } catch (Exception e) {
                logger.error("Exception getting nodeFsStats for all nodes.", e);
                logger.warn("Returning cached value for nodeFsStats.");
            }
        } else {
            logger.info("Returning cached NodesStatsResponse, age: {}ms", cachediff);
        }

        return this.cachedNodeStats;
    }


    /**
     * Check if there is enough disk space for more shards on the node
     *
     * @param nodeId node to check disk space of
     * @param minimumAvailablePercentage
     * @return true if the node is below the threshold, false if not
     */
    public boolean enoughDiskForShard(final String nodeId, double minimumAvailablePercentage) {
        logger.info("enoughDiskForShard on {}.", nodeId);

        boolean enoughSpace = true;
        NodesStatsResponse nodeStats;
        nodeStats = this.cachedNodeFsStats();
        if (null == nodeStats) {
            logger.warn("Unable to check for enough disk space, nodeStats were not provided. Failing open (true).");
            return true;
        }

        Map<String, NodeStats> nodeStatsMap = nodeStats.getNodesMap();
        if (!nodeStatsMap.containsKey(nodeId)) {
            logger.debug("Node not included in the node stats, resetting ttl and retrieving NodeFsStats again...");
            this.lastNodeStatsTime = 0L;
            nodeStats = this.cachedNodeFsStats();
            nodeStatsMap = nodeStats.getNodesMap();
        }
        NodeStats ns = nodeStatsMap.get(nodeId);
        FsStats fs = ns.getFs();
        List<Double> percents = percentagesFree(fs);
        for (Double p : percents) {
            if (p < minimumAvailablePercentage) {
                logger.info("Throttling shard allocation due to excessive disk use ({} < {} % free) on {}",
                        p, minimumAvailablePercentage, nodeId);
                enoughSpace = false;
            }
        }
        return enoughSpace;
    }


    /**
     * Takes a FsStats object and returns the list of percentages of Free disk for the filesystem stats
     *
     * @param fs FsStats object to return percentages for
     * @return a List of Doubles representing percentage free values
     */
    public List<Double> percentagesFree(final FsStats fs) {
        List<Double> results = new ArrayList<Double>();
        Iterator<FsStats.Info> i = fs.iterator();
        while (i.hasNext()) {
            FsStats.Info stats = i.next();
            double percentFree = ((double)stats.getAvailable().bytes() / (double)stats.getTotal().bytes()) * 100.0;
            logger.info("Space: {} bytes available, {} total bytes. Percent free: [{} %]",
                    stats.getAvailable().bytes(), stats.getTotal().bytes(), percentFree);
            results.add(percentFree);
        }

        return results;
    }

    /**
     * Retrieves the shard sizes for all shards in the cluster, returns null
     * if an exception occurs.
     *
     * @return a Map of ShardId to size in bytes of the shard
     */
    public HashMap<ShardId, Long> nodeShardStats() {
        logger.trace("nodeShardStats");
        final HashMap<ShardId, Long> shardSizes = new HashMap<ShardId, Long>();
        IndicesStatsRequest request = new IndicesStatsRequest();
        request.clear();
        request.store(true);
        IndicesStatsResponse resp;

        try {
            resp = indicesStatsAction.execute(request).actionGet(shardStatsTimeout);
        } catch (Exception e) {
            logger.error("Exception getting shard stats for each node.", e);
            return null;
        }
        for (ShardStats stats : resp.getShards()) {
            shardSizes.put(stats.getShardRouting().shardId(), stats.getStats().getStore().getSizeInBytes());
        }
        return shardSizes;
    }


    /**
     * Averages the available free percentage for a FsStats object
     *
     * @param fs object to average free bytes for
     * @return the average available bytes for all mount points
     */
    public double averagePercentageFree(final FsStats fs) {
        double totalPercentages = 0;
        int statNum = 0;
        Iterator<FsStats.Info> i = fs.iterator();
        while (i.hasNext()) {
            FsStats.Info stats = i.next();
            double percentFree = 100.0 * ((double)stats.getAvailable().bytes() / (double)stats.getTotal().bytes());
            logger.trace("pFree: {} [{} / {}]", percentFree, stats.getAvailable().bytes(), stats.getTotal().bytes());
            totalPercentages += percentFree;
            statNum++;
        }
        double avg = (totalPercentages / statNum);
        logger.trace("Average percentage free: " + avg);
        return avg;
    }

    /**
     * Sort nodes by the number of shards on each, lowest to highest
     *
     * @param allocation allocation of shards in the cluster
     * @return an array of nodes sorted by lowest to highest shard count
     */
    public RoutingNode[] sortedNodesByShardCountLeastToHigh(final RoutingAllocation allocation) {
        // create count per node id, taking into account relocations
        final TObjectIntHashMap<String> nodeCounts = new TObjectIntHashMap<String>();
        for (RoutingNode node : allocation.routingNodes()) {
            for (int i = 0; i < node.shards().size(); i++) {
                ShardRouting shardRouting = node.shards().get(i);
                String nodeId = shardRouting.relocating() ? shardRouting.relocatingNodeId() : shardRouting.currentNodeId();
                nodeCounts.adjustOrPutValue(nodeId, 1, 1);
            }
        }

        RoutingNode[] nodes = allocation.routingNodes().nodesToShards().values().toArray(new RoutingNode[allocation.routingNodes().nodesToShards().values().size()]);
        Arrays.sort(nodes, new Comparator<RoutingNode>() {
            @Override
            public int compare(RoutingNode o1, RoutingNode o2) {
                int c1 = nodeCounts.get(o1.nodeId());
                int c2 = nodeCounts.get(o2.nodeId());
                logger.trace("comparing {}[{}] to {}[{}]",
                        o1.nodeId(), c1,
                        o2.nodeId(), c2);
                return (c1 - c2);
            }
        });
        return nodes;
    }

    public RoutingNode[] sortedNodesByFreeSpaceLeastToHigh(final RoutingAllocation allocation,
                                                            final NodesStatsResponse nodeStats) {
        RoutingNode[] nodes = allocation.routingNodes().nodesToShards().values().toArray(new RoutingNode[allocation.routingNodes().nodesToShards().values().size()]);

        if (logger.isTraceEnabled()) {
            for (RoutingNode node : nodes) {
                logger.trace("node: {} -> {}", node.nodeId(),
                        averageAvailableBytes(nodeStats.getNodesMap().get(node.nodeId()).getFs()));
            }
        }

        Arrays.sort(nodes, new Comparator<RoutingNode>() {
            @Override
            public int compare(RoutingNode o1, RoutingNode o2) {
                FsStats fs1 = nodeStats.getNodesMap().get(o1.nodeId()).getFs();
                FsStats fs2 = nodeStats.getNodesMap().get(o2.nodeId()).getFs();
                double avgAvailable1 = averagePercentageFree(fs1);
                double avgAvailable2 = averagePercentageFree(fs2);
                logger.trace("{}[{}%] vs {}[{}%]",
                        o1.nodeId(), avgAvailable1,
                        o2.nodeId(), avgAvailable2);
                double sizeDiff = avgAvailable1 - avgAvailable2;
                if (sizeDiff > 0) {
                    return 1;
                } if (sizeDiff == 0) {
                    return 0;
                } else {
                    return -1;
                }
            }
        });

        if (logger.isTraceEnabled()) {
            for (RoutingNode node : nodes) {
                logger.trace("SortedNode: {} -> {}", node.nodeId(),
                        averagePercentageFree(nodeStats.getNodesMap().get(node.nodeId()).getFs()));
            }
        }

        return nodes;

    }


    /**
     * Averages the available free bytes for a FsStats object
     *
     * @param fs object to average free bytes for
     * @return the average available bytes for all mount points
     */
    public long averageAvailableBytes(final FsStats fs) {
        long totalAvail = 0;
        int statNum = 0;
        Iterator<FsStats.Info> i = fs.iterator();
        while (i.hasNext()) {
            FsStats.Info stats = i.next();
            totalAvail += stats.getAvailable().bytes();
            statNum++;
        }
        long avg = (totalAvail / statNum);
        return avg;
    }



    /**
     * Determine whether a shard swap should be attempted
     *
     * @param allocation RoutingAllocation for the cluster
     * @param stats NodeStatsResponse containing FsStats for each node in the cluster
     * @return true if a swap should be attempted, false otherwise
     */
    public boolean eligibleForSwap(final RoutingAllocation allocation, final NodesStatsResponse stats) {
        // Skip if only one node is present in the cluster
        if (allocation.nodes().size() == 1) {
            logger.info("Only one node in the cluster, skipping shard swap check.");
            return false;
        }

        if (stats == null) {
            logger.warn("Unable to determine nodeFsStats, aborting shardSwap.");
            return false;
        }
        return true;
    }

    /**
     * Given a 'larger' node, a 'smaller' node and stats about the nodes,
     * determine whether the nodes differ enough in used disk space to warrant
     * a shard swap.
     *
     * @param largeNode Node presumed to be larger
     * @param smallNode Node presumed to be smaller
     * @param stats NodeFsStats containing FsStats about the nodes
     * @return true if the nodes differ enough, false otherwise
     */
    public boolean nodesDifferEnoughToSwap(final RoutingNode largeNode, final RoutingNode smallNode,
                                           final NodesStatsResponse stats, double minimumSwapDifferencePercentage) {
        Map<String, NodeStats> nodeStats = stats.getNodesMap();
        double largeNodeUsedSize = 100 - this.averagePercentageFree(nodeStats.get(largeNode.nodeId()).getFs());
        double smallNodeUsedSize = 100 - this.averagePercentageFree(nodeStats.get(smallNode.nodeId()).getFs());

        double sizeDifference = largeNodeUsedSize - smallNodeUsedSize;

        logger.info("Checking size disparity: {}[{} % used] -> {}[{} % used] ({} >= {})",
                largeNode.nodeId(), largeNodeUsedSize,
                smallNode.nodeId(), smallNodeUsedSize,
                sizeDifference, minimumSwapDifferencePercentage);
        return (sizeDifference >= minimumSwapDifferencePercentage);
    }


    /**
     * Checks two shards to see whether they differ enough in size that they
     * should be swapped
     *
     * @param largeShard Large shard to be considered for swapping
     * @param smallShard Small shard to be considered for swapping
     * @param shardSizes Map of shardId to shard size for size comparison
     * @return true if the shards should be swapped, false otherwise
     */
    public boolean shardsDifferEnoughToSwap(final MutableShardRouting largeShard,
                                            final MutableShardRouting smallShard,
                                            final HashMap<ShardId, Long> shardSizes,
                                            double minimumSwapShardRelativeDifferencePercentage) {
        // If we weren't able to find shards that could be swapped, just
        // bail out early
        if (largeShard == null || smallShard == null) {
            logger.info("Unable to find shards to swap. [deciders]");
            return false;
        }

        // If we've gone through the list, and the 'larger' shard is
        // smaller than the 'smaller' shard, don't bother swapping
        long largeSize = shardSizes.get(largeShard.shardId());
        long smallSize = shardSizes.get(smallShard.shardId());

        logger.info("Swappable shards found.");

        logger.info("large: {}[{} bytes], small: {}[{} bytes]",
                largeShard, largeSize, smallShard, smallSize);

        if (largeSize == 0 || smallSize == 0) {
            logger.warn("Unable to find shards to swap. [shard size 0?!]");
            return false;
        }

        // check to make sure it's actually worth swapping shards, the size
        // disparity should be large enough to make a difference
        double shardRelativeSize = (100.0 * smallSize / largeSize);
        if (shardRelativeSize > minimumSwapShardRelativeDifferencePercentage) {
            logger.info("Unable to find suitable shards to swap. Smallest shard {}% of large shard size, must be <= {}%",
                    shardRelativeSize, minimumSwapShardRelativeDifferencePercentage);
            return false;
        }
        logger.info("Small shard is {} % of large shard size, below swapping threshold of {} %.",
                shardRelativeSize, minimumSwapShardRelativeDifferencePercentage);

        return true;
    }


    /**
     * Returns the first shard out of a list that can be allocated to the 'to'
     * node. Returns null if no shard can be found.
     *
     * @param allocation RoutingAllocation of the cluster
     * @param shards list of shards to check for relocatibility
     * @param to RoutingNode to check shard relocation against
     * @return shard that can be relocated
     */
    public MutableShardRouting firstShardThatCanBeRelocated(final RoutingAllocation allocation,
                                                            final List<MutableShardRouting> shards,
                                                            final RoutingNode to) {
        MutableShardRouting resultShard = null;

        for (MutableShardRouting shard : shards) {
            logger.debug("Checking deciders for {}...", shard);
            if (allocation.deciders().canAllocate(shard, to, allocation).type() == Decision.Type.YES) {
                resultShard = shard;
                logger.debug("Deciders have OKed {} for swapping.", resultShard);
                break;
            }
        }

        return resultShard;
    }


    /**
     * The shardSwap method checks to see whether the largest and smallest
     * nodes are too far apart from each other, in terms of disk space
     * percentage, and attempts to swap a large shard from the loaded node
     * for a small shard from the underloaded node
     *
     * @param allocation current shard allocation for the cluster
     * @return true if shards were swapped, false otherwise
     */
    public boolean shardSwap(RoutingAllocation allocation, double minimumSwapDifferencePercentage, double minimumSwapShardRelativeDifferencePercentage) {
        boolean changed = false;

        logger.info("Initiating shard swap check.");

        NodesStatsResponse stats = this.nodeFsStats();
        if (!eligibleForSwap(allocation, stats)) {
            return changed;
        }

        RoutingNode[] nodesSmallestToLargest = this.sortedNodesByFreeSpaceLeastToHigh(allocation, stats);
        if (logger.isDebugEnabled()) {
            for (RoutingNode node : nodesSmallestToLargest) {
                logger.debug("Node: {} -> {} % used", node.nodeId(),
                        (100 - this.averagePercentageFree(stats.getNodesMap().get(node.nodeId()).getFs())));
            }
        }

        RoutingNode largestNode = nodesSmallestToLargest[0];
        RoutingNode smallestNode = nodesSmallestToLargest[nodesSmallestToLargest.length - 1];

        if (nodesDifferEnoughToSwap(largestNode, smallestNode, stats, minimumSwapDifferencePercentage)) {
            logger.info("Size disparity found, checking for swappable shards.");
            HashMap<ShardId, Long> shardSizes = this.nodeShardStats();

            // If unable to retrieve shard sizes, abort quickly because
            // something is probably wrong with the cluster
            if (shardSizes == null) {
                logger.warn("Unable to retrieve shard sizes, aborting shard swap.");
                return changed;
            }

            // generate a list of shards on the largest node, largest shard first
            List<MutableShardRouting> largestNodeShards = sortedStartedShardsOnNodeLargestToSmallest(largestNode, shardSizes);

            // generate a list of shards on the smallest node, smallest shard first
            List<MutableShardRouting> smallestNodeShards = sortedStartedShardsOnNodeLargestToSmallest(smallestNode, shardSizes);
            Collections.reverse(smallestNodeShards);

            if (logger.isTraceEnabled()) {
                for (MutableShardRouting shard : largestNodeShards) {
                    logger.trace("[large] shard {} => {}", shard.shardId(), shardSizes.get(shard.shardId()));
                }
                for (MutableShardRouting shard : smallestNodeShards) {
                    logger.trace("[small] shard {} => {}", shard.shardId(), shardSizes.get(shard.shardId()));
                }
            }

            MutableShardRouting largestShardAvailableForRelocation =
                    firstShardThatCanBeRelocated(allocation, largestNodeShards, smallestNode);
            MutableShardRouting smallestShardAvailableForRelocation =
                    firstShardThatCanBeRelocated(allocation, smallestNodeShards, largestNode);

            if (!shardsDifferEnoughToSwap(largestShardAvailableForRelocation,
                    smallestShardAvailableForRelocation, shardSizes, minimumSwapShardRelativeDifferencePercentage)) {
                return changed;
            }

            // swap the two shards
            logger.info("Swapping ({}) and ({})",
                    smallestShardAvailableForRelocation,
                    largestShardAvailableForRelocation);
            largestNode.add(new MutableShardRouting(smallestShardAvailableForRelocation.index(),
                    smallestShardAvailableForRelocation.id(),
                    largestNode.nodeId(),
                    smallestShardAvailableForRelocation.currentNodeId(),
                    smallestShardAvailableForRelocation.primary(),
                    INITIALIZING,
                    smallestShardAvailableForRelocation.version() + 1));
            largestShardAvailableForRelocation.relocate(smallestNode.nodeId());

            smallestNode.add(new MutableShardRouting(largestShardAvailableForRelocation.index(),
                    largestShardAvailableForRelocation.id(),
                    smallestNode.nodeId(),
                    largestShardAvailableForRelocation.currentNodeId(),
                    largestShardAvailableForRelocation.primary(),
                    INITIALIZING,
                    largestShardAvailableForRelocation.version() + 1));
            smallestShardAvailableForRelocation.relocate(largestNode.nodeId());
            changed = true;
        }

        logger.info("Finished shard swap. " + (changed ? "Swap performed." : "Swap skipped."));
        return changed;
    }


    /**
     * Sorts shards for a node, based on estimated size of the shard
     *
     * @param node RoutingNode to sort shards for
     * @return sorted list of MutableShardRouting shards, sorted by size with the largest first
     */
    public List<MutableShardRouting> sortedStartedShardsOnNodeLargestToSmallest(final RoutingNode node,
                                                                                final Map<ShardId, Long> shardSizes) {
        logger.trace("sortedStartedShardsOnNode");

        List<MutableShardRouting> shards = node.shardsWithState(STARTED);
        final HashMap<MutableShardRouting, Long> sizeMap = new HashMap<MutableShardRouting, Long>();
        for (MutableShardRouting shard : shards) {
            sizeMap.put(shard, shardSizes.get(shard.shardId()));
        }

        Collections.sort(shards, new Comparator<MutableShardRouting>() {
            @Override
            public int compare(MutableShardRouting msr1, MutableShardRouting msr2) {
                long sizeDiff = sizeMap.get(msr2) - sizeMap.get(msr1);
                if (sizeDiff > 0) {
                    return 1;
                } if (sizeDiff == 0) {
                    return 0;
                } else {
                    return -1;
                }
            }
        });
        return shards;
    }

}
