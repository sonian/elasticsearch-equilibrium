package com.sonian.elasticsearch.equilibrium;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.fs.FsStats;

import java.util.*;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

/**
 * DiskShardsAllocator is a copy of the stock-ES EvenCountShardsAllocator,
 * but checks (in addition) that nodes are not above a certain threshold,
 * and refuses to relocate shards to them if they have disk usage above this.
 *
 * In addition, it provides the shardSwap method, to swap shards between an
 * overloaded and underloaded node.
 *
 * @author dakrone
 */
public class DiskShardsAllocator extends AbstractComponent implements ShardsAllocator {

    // helper providing shard size and fsStats options
    private final NodeInfoHelper nodeInfoHelper;

    // the minimum percentage of free disk space before we stop sending shards
    // to a node
    private final double minimumAvailablePercentage;

    // the minimum difference between the largest and smallest nodes before
    // a shard swap is attempted
    private final double minimumSwapDifferencePercentage;

    // configurable difference between large and small shards to swap
    private final double minimumSwapShardRelativeDifferencePercentage;

    @Inject
    public DiskShardsAllocator(Settings settings, NodeInfoHelper nodeInfoHelper) {
        super(settings);
        this.nodeInfoHelper = nodeInfoHelper;

        Settings compSettings = settings.getComponentSettings(this.getClass());

        // read in configurable minimum percentage; defaults to 20% free
        // minimum for throttling
        this.minimumAvailablePercentage = compSettings.getAsDouble("minimumAvailablePercentage", 20.0);
        // read in configurable difference before shards are swapped, defaults
        // to 20% difference
        this.minimumSwapDifferencePercentage = compSettings.getAsDouble("minimumSwapDifferencePercentage", 5.0);
        // read in configurable difference between large and small shards
        // to swap
        this.minimumSwapShardRelativeDifferencePercentage =  compSettings.getAsDouble("minimumSwapShardRelativeDifferencePercentage", 75.0);
    }


    /**
     * We override the applyStartedShards method only to add logging, the
     * original method in EvenShardsCountAllocator doesn't do anything
     */
    @Override
    public void applyStartedShards(StartedRerouteAllocation allocation) {
        logger.trace("applyStartedShards");
    }


    /**
     * We override the applyFailedShards method only to add logging, the
     * original method in EvenShardsCountAllocator doesn't do anything
     */
    @Override
    public void applyFailedShards(FailedRerouteAllocation allocation) {
        logger.trace("applyFailedShards");
    }


    /**
     * This allocateUnassigned method is almost identical to the
     * EvenShardsCountAllocator, however, instead of only checking the
     * allocation deciders, it also checks that there is enough disk space
     * for the shard on the node
     *
     * @param allocation the current routing allocation for the cluster
     * @return a boolean indicating whether the routing of the cluster has been changed
     */
    @Override
    public boolean allocateUnassigned(RoutingAllocation allocation) {
        logger.trace("allocateUnassigned");

        boolean changed = false;
        RoutingNodes routingNodes = allocation.routingNodes();
        NodesStatsResponse stats = this.nodeInfoHelper.nodeFsStats();
        if (stats == null) {
            logger.warn("Unable to determine nodeFsStats, aborting allocation.");
            return false;
        }

        RoutingNode[] nodes = sortedNodesByShardCountLeastToHigh(allocation);

        Iterator<MutableShardRouting> unassignedIterator = routingNodes.unassigned().iterator();
        int lastNode = 0;

        while (unassignedIterator.hasNext()) {
            MutableShardRouting shard = unassignedIterator.next();
            // do the equilibrium, finding the least "busy" node
            for (int i = 0; i < nodes.length; i++) {
                RoutingNode node = nodes[lastNode];
                lastNode++;
                if (lastNode == nodes.length) {
                    lastNode = 0;
                }

                // Here is our added bit. Where, in addition to checking the
                // allocation deciders, we check there is enough disk space
                if (allocation.deciders().canAllocate(shard, node, allocation).allocate() &&
                        this.enoughDiskForShard(shard, node, stats)) {

                    int avgShardCount = routingNodes.requiredAverageNumberOfShardsPerNode();
                    int numberOfShardsToAllocate = avgShardCount - node.shards().size();
                    if (numberOfShardsToAllocate <= 0) {
                        continue;
                    }
                    logger.trace("Need {} shards on {} to reach the average shards count ({})",
                                 numberOfShardsToAllocate, node.nodeId(), avgShardCount);

                    changed = true;
                    node.add(shard);
                    unassignedIterator.remove();
                    break;
                }
            }
        }

        // allocate all the unassigned shards above the average per node.
        for (Iterator<MutableShardRouting> it = routingNodes.unassigned().iterator(); it.hasNext(); ) {
            MutableShardRouting shard = it.next();
            // go over the nodes and try and allocate the remaining ones
            for (RoutingNode routingNode : sortedNodesByShardCountLeastToHigh(allocation)) {
                if (allocation.deciders().canAllocate(shard, routingNode, allocation).allocate() &&
                        this.enoughDiskForShard(shard, routingNode, stats)) {
                    changed = true;
                    routingNode.add(shard);
                    it.remove();
                    break;
                }
            }
        }
        return changed;
    }


    /**
     * This rebalance method is almost identical to the
     * EvenShardsCountAllocator, however, instead of only checking the
     * allocation deciders, it also checks that there is enough disk space
     * for the shard on the node
     *
     * @param allocation the current routing allocation for the cluster
     * @return a boolean indicating whether the routing of the cluster has been changed
     */
    @Override
    public boolean rebalance(RoutingAllocation allocation) {
        logger.trace("rebalance");
        boolean changed = false;
        if (allocation.nodes().getSize() == 0) {
            return false;
        }

        NodesStatsResponse stats = this.nodeInfoHelper.nodeFsStats();
        if (stats == null) {
            logger.warn("Unable to determine nodeFsStats, aborting rebalance.");
            return false;
        }

        RoutingNode[] sortedNodesLeastToHigh = sortedNodesByShardCountLeastToHigh(allocation);
        int lowIndex = 0;
        int highIndex = sortedNodesLeastToHigh.length - 1;
        boolean relocationPerformed;
        do {
            relocationPerformed = false;
            while (lowIndex != highIndex) {
                RoutingNode lowRoutingNode = sortedNodesLeastToHigh[lowIndex];
                RoutingNode highRoutingNode = sortedNodesLeastToHigh[highIndex];
                int averageNumOfShards = allocation.routingNodes().requiredAverageNumberOfShardsPerNode();

                // only active shards can be removed so must count only active ones.
                if (highRoutingNode.numberOfOwningShards() <= averageNumOfShards) {
                    highIndex--;
                    continue;
                }

                if (lowRoutingNode.shards().size() >= averageNumOfShards) {
                    lowIndex++;
                    continue;
                }

                boolean relocated = false;

                List<MutableShardRouting> startedShards = highRoutingNode.shardsWithState(STARTED);
                for (MutableShardRouting startedShard : startedShards) {
                    if (!allocation.deciders().canRebalance(startedShard, allocation)) {
                        continue;
                    }

                    // in addition to checking the deciders, the shard disk is
                    // checked to ensure it is not above the threshold
                    if (allocation.deciders().canAllocate(startedShard, lowRoutingNode, allocation).allocate() &&
                            this.enoughDiskForShard(startedShard, lowRoutingNode, stats)) {
                        changed = true;
                        lowRoutingNode.add(new MutableShardRouting(startedShard.index(), startedShard.id(),
                                lowRoutingNode.nodeId(), startedShard.currentNodeId(),
                                startedShard.primary(), INITIALIZING, startedShard.version() + 1));

                        startedShard.relocate(lowRoutingNode.nodeId());
                        relocated = true;
                        relocationPerformed = true;
                        break;
                    }
                }

                if (!relocated) {
                    highIndex--;
                }
            }
        } while (relocationPerformed);

        return changed;
    }


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
                                           final NodesStatsResponse stats) {
        Map<String, NodeStats> nodeStats = stats.getNodesMap();
        double largeNodeUsedSize = 100 - averagePercentageFree(nodeStats.get(largeNode.nodeId()).fs());
        double smallNodeUsedSize = 100 - averagePercentageFree(nodeStats.get(smallNode.nodeId()).fs());

        double sizeDifference = largeNodeUsedSize - smallNodeUsedSize;

        logger.info("Checking size disparity: {}[{} % used] -> {}[{} % used] ({} >= {})",
                largeNode.nodeId(), largeNodeUsedSize,
                smallNode.nodeId(), smallNodeUsedSize,
                sizeDifference, this.minimumSwapDifferencePercentage);
        return (sizeDifference >= this.minimumSwapDifferencePercentage);
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
    public boolean shardSwap(RoutingAllocation allocation) {
        boolean changed = false;

        logger.info("Initiating shard swap check.");

        NodesStatsResponse stats = this.nodeInfoHelper.nodeFsStats();
        if (!eligibleForSwap(allocation, stats)) {
            return changed;
        }

        RoutingNode[] nodesSmallestToLargest = sortedNodesByFreeSpaceLeastToHigh(allocation, stats);
        for (RoutingNode node : nodesSmallestToLargest) {
            logger.debug("Node: {} -> {} % used", node.nodeId(),
                         (100 - averagePercentageFree(stats.getNodesMap().get(node.nodeId()).fs())));
        }

        RoutingNode largestNode = nodesSmallestToLargest[0];
        RoutingNode smallestNode = nodesSmallestToLargest[nodesSmallestToLargest.length - 1];

        if (nodesDifferEnoughToSwap(largestNode, smallestNode, stats)) {
            logger.info("Size disparity found, checking for swappable shards.");
            HashMap<ShardId, Long> shardSizes = this.nodeInfoHelper.nodeShardStats();

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

            MutableShardRouting largestShardAvailableForRelocation = null;
            MutableShardRouting smallestShardAvailableForRelocation = null;

            if (logger.isTraceEnabled()) {
                for (MutableShardRouting shard : largestNodeShards) {
                    logger.trace("[large] shard {} => {}", shard.shardId(), shardSizes.get(shard.shardId()));
                }
            }

            // check if we can find a shard to relocate from the largest to
            // smallest node
            for (MutableShardRouting shard : largestNodeShards) {
                logger.debug("[large] Checking deciders for {}...", shard);
                if (allocation.deciders().canAllocate(shard, smallestNode, allocation).allocate()) {
                    largestShardAvailableForRelocation = shard;
                    logger.debug("[large] Deciders have OKed {} for swapping.",
                                 largestShardAvailableForRelocation);
                    break;
                }
            }

            if (logger.isTraceEnabled()) {
                for (MutableShardRouting shard : smallestNodeShards) {
                    logger.trace("[small] shard {} => {}", shard.shardId(), shardSizes.get(shard.shardId()));
                }
            }

            // check if we can find a shard to relocate from the smallest to
            // largest node
            for (MutableShardRouting shard : smallestNodeShards) {
                logger.debug("[small] Checking deciders for {}...", shard);
                if (allocation.deciders().canAllocate(shard, largestNode, allocation).allocate()) {
                    smallestShardAvailableForRelocation = shard;
                    logger.debug("[small] Deciders have OKed {} for swapping.",
                                 smallestShardAvailableForRelocation);
                    break;
                }
            }

            // If we weren't able to find shards that could be swapped, just
            // bail out early
            if (largestShardAvailableForRelocation == null || smallestShardAvailableForRelocation == null) {
                logger.info("Unable to find shards to swap. [deciders]");
                return changed;
            }

            // If we've gone through the list, and the 'larger' shard is
            // smaller than the 'smaller' shard, don't bother swapping
            long largeSize = shardSizes.get(largestShardAvailableForRelocation.shardId());
            long smallSize = shardSizes.get(smallestShardAvailableForRelocation.shardId());

            logger.info("Swappable shards found.");

            logger.info("large: {}[{} bytes], small: {}[{} bytes]",
                        largestShardAvailableForRelocation, largeSize,
                        smallestShardAvailableForRelocation, smallSize);

            if (largeSize == 0 || smallSize == 0) {
                logger.warn("Unable to find shards to swap. [shard size 0?!]");
                return changed;
            }

            // check to make sure it's actually worth swapping shards, the size
            // disparity should be large enough to make a difference
            double shardRelativeSize = (100.0 * smallSize / largeSize);
            if (shardRelativeSize > this.minimumSwapShardRelativeDifferencePercentage) {
                logger.info("Unable to find suitable shards to swap. Smallest shard {}% of large shard size, must be <= {}%",
                            shardRelativeSize, this.minimumSwapShardRelativeDifferencePercentage);
                return changed;
            }
            logger.info("Small shard is {} % of large shard size, below swapping threshold of {} %.",
                        shardRelativeSize, this.minimumSwapShardRelativeDifferencePercentage);

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
     * This move method is almost identical to the EvenShardsCountAllocator,
     * however, instead of only checking the allocation deciders, it also
     * checks that there is enough disk space for the shard on the node
     *
     * @param shardRouting shard to be moved
     * @param node node to move the shard from
     * @param allocation routing layout for the cluster
     * @return true if shard routing has been changed, false if not
     */
    @Override
    public boolean move(MutableShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        logger.trace("move");
        assert shardRouting.started();
        boolean changed = false;
        if (allocation.nodes().getSize() == 0) {
            return false;
        }
        NodesStatsResponse stats = this.nodeInfoHelper.nodeFsStats();
        if (stats == null) {
            logger.warn("Unable to determine nodeFsStats, aborting shard move.");
            return false;
        }

        RoutingNode[] sortedNodesLeastToHigh = sortedNodesByShardCountLeastToHigh(allocation);

        for (RoutingNode nodeToCheck : sortedNodesLeastToHigh) {
            // check if its the node we are moving from, no sense to check on it
            if (nodeToCheck.nodeId().equals(node.nodeId())) {
                continue;
            }

            // in addition to checking the deciders, we check that we are not
            // above the disk threshold on the node
            if (allocation.deciders().canAllocate(shardRouting, nodeToCheck, allocation).allocate() &&
                    this.enoughDiskForShard(shardRouting, nodeToCheck, stats)) {
                nodeToCheck.add(new MutableShardRouting(shardRouting.index(), shardRouting.id(),
                        nodeToCheck.nodeId(), shardRouting.currentNodeId(),
                        shardRouting.primary(), INITIALIZING, shardRouting.version() + 1));

                shardRouting.relocate(nodeToCheck.nodeId());
                changed = true;
                break;
            }
        }

        return changed;
    }


    /**
     * Sort nodes by the number of shards on each, lowest to highest
     *
     * @param allocation allocation of shards in the cluster
     * @return an array of nodes sorted by lowest to highest shard count
     */
    private RoutingNode[] sortedNodesByShardCountLeastToHigh(final RoutingAllocation allocation) {
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

    private RoutingNode[] sortedNodesByFreeSpaceLeastToHigh(final RoutingAllocation allocation,
                                                            final NodesStatsResponse nodeStats) {
        RoutingNode[] nodes = allocation.routingNodes().nodesToShards().values().toArray(new RoutingNode[allocation.routingNodes().nodesToShards().values().size()]);

        if (logger.isTraceEnabled()) {
            for (RoutingNode node : nodes) {
                logger.trace("node: {} -> {}", node.nodeId(),
                             averageAvailableBytes(nodeStats.getNodesMap().get(node.nodeId()).fs()));
            }
        }

        Arrays.sort(nodes, new Comparator<RoutingNode>() {
            @Override
            public int compare(RoutingNode o1, RoutingNode o2) {
                FsStats fs1 = nodeStats.getNodesMap().get(o1.nodeId()).fs();
                FsStats fs2 = nodeStats.getNodesMap().get(o2.nodeId()).fs();
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
                             averagePercentageFree(nodeStats.getNodesMap().get(node.nodeId()).fs()));
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
            totalAvail += stats.available().bytes();
            statNum++;
        }
        long avg = (totalAvail / statNum);
        return avg;
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
            double percentFree = 100.0 * ((double)stats.available().bytes() / (double)stats.total().bytes());
            logger.trace("pFree: {} [{} / {}]", percentFree, stats.available().bytes(), stats.total().bytes());
            totalPercentages += percentFree;
            statNum++;
        }
        double avg = (totalPercentages / statNum);
        logger.trace("Average percentage free: " + avg);
        return avg;
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
            double percentFree = ((double)stats.available().bytes() / (double)stats.total().bytes()) * 100.0;
            logger.info("Space: {} bytes available, {} total bytes. Percent free: [{} %]",
                        stats.available().bytes(), stats.total().bytes(), percentFree);
            results.add(percentFree);
        }

        return results;
    }


    /**
     * Check if there is enough disk space for more shards on the node
     *
     * @param shard shard that is being checked
     * @param routingNode node to check disk space of
     * @param nodeStats the NodesStatsResponse for FS stats of the cluster
     * @return true if the node is below the threshold, false if not
     */
    public boolean enoughDiskForShard(final MutableShardRouting shard, final RoutingNode routingNode,
                                      final NodesStatsResponse nodeStats) {
        boolean enoughSpace = true;
        
        logger.info("enoughDiskForShard on {} for: {}", routingNode.nodeId(), shard.shardId());

        String nodeId = routingNode.nodeId();
        Map<String, NodeStats> nodeStatsMap = nodeStats.getNodesMap();
        NodeStats ns = nodeStatsMap.get(nodeId);
        FsStats fs = ns.fs();
        List<Double> percents = percentagesFree(fs);
        for (Double p : percents) {
            if (p < this.minimumAvailablePercentage) {
                logger.info("Throttling shard allocation due to excessive disk use ({} < {} % free) on {}",
                            p, this.minimumAvailablePercentage, nodeId);
                enoughSpace = false;
            }
        }
        return enoughSpace;
    }
}
