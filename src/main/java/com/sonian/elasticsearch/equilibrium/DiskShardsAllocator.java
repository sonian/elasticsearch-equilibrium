package com.sonian.elasticsearch.equilibrium;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.fs.FsStats;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

/**
 * DiskShardsAllocator is a copy of the stock-ES EvenCountShardsAllocator,
 * but checks (in addition) that nodes are not above a certain threshold,
 * and refuses to relocate shards to them if they have disk usage above this
 *
 * @author dakrone
 */
public class DiskShardsAllocator extends AbstractComponent implements ShardsAllocator {

    // action used to gather FsStats for all the nodes in the cluster, to check
    // their disk usage
    private final TransportNodesStatsAction nodesStatsAction;

    // the minimum percentage of free disk space before we stop sending shards
    // to a node
    private final double minimumAvailablePercentage;

    // service used to retrieve an InternalIndexShard to estimate the size of
    // the shard in bytes
    private final IndicesService indicesService;

    @Inject
    public DiskShardsAllocator(Settings settings, TransportNodesStatsAction nodesStatsAction, IndicesService indicesService) {
        super(settings);
        this.nodesStatsAction = nodesStatsAction;
        this.indicesService = indicesService;

        // read in configurable minimum percentage; defaults to 20% free minimum
        this.minimumAvailablePercentage = settings.getComponentSettings(this.getClass()).getAsDouble("minimumAvailablePercentage", 20.0);
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
        logger.info("allocateUnassigned");

        boolean changed = false;
        RoutingNodes routingNodes = allocation.routingNodes();
        NodesStatsResponse stats = nodeFsStats();

        RoutingNode[] nodes = sortedNodesLeastToHigh(allocation, stats);

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
                    int numberOfShardsToAllocate = routingNodes.requiredAverageNumberOfShardsPerNode() - node.shards().size();
                    if (numberOfShardsToAllocate <= 0) {
                        continue;
                    }

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
            for (RoutingNode routingNode : sortedNodesLeastToHigh(allocation,stats)) {
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
     * shardSize attempts to retrieve the size of the shard in bytes
     *
     * @param shardRouting shard routing for the shard
     * @return the size of the shard in bytes, or -1 if the estimate failed
     */
    public long shardSize(MutableShardRouting shardRouting) {
        ShardId shardId = shardRouting.shardId();

        InternalIndexService indexService = (InternalIndexService) indicesService.indexServiceSafe(shardRouting.getIndex());
        InternalIndexShard indexShard = (InternalIndexShard) indexService.shardSafe(shardId.getId());

        long shardSize;

        try {
            shardSize = indexShard.store().estimateSize().bytes();
            logger.info("shard: " + shardId + " space: " + shardSize);
        } catch (IOException e) {
            logger.info("Unable to get estimated size for " + shardId + ": " + e.getMessage());
            return -1;
        }

        return shardSize;
    }

    /**
     * Sorts shards for a node, based on estimated size of the shard
     *
     * @param node RoutingNode to sort shards for
     * @return sorted list of MutableShardRouting shards, sorted by size with the largest first
     */
    public List<MutableShardRouting> sortedStartedShardsOnNode(RoutingNode node) {
        List<MutableShardRouting> shards = node.shardsWithState(STARTED);
        Collections.sort(shards, new Comparator<MutableShardRouting>() {
            @Override
            public int compare(MutableShardRouting msr1, MutableShardRouting msr2) {
                return (int) (shardSize(msr2) - shardSize(msr1));
            }
        });
        return shards;
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
        logger.info("rebalance");
        boolean changed = false;
        if (allocation.nodes().getSize() == 0) {
            return false;
        }

        NodesStatsResponse stats = nodeFsStats();
        RoutingNode[] sortedNodesLeastToHigh = sortedNodesLeastToHigh(allocation, stats);
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

                // instead of getting shards randomly from the node, we now
                // sort them according to their size, largest first
                List<MutableShardRouting> startedShards = sortedStartedShardsOnNode(highRoutingNode);
//                for (MutableShardRouting s : startedShards) {
//                    logger.info("ss:" + s.shardId());
//                }

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
        logger.info("move");
        assert shardRouting.started();
        boolean changed = false;
        if (allocation.nodes().getSize() == 0) {
            return false;
        }
        NodesStatsResponse stats = nodeFsStats();
        RoutingNode[] sortedNodesLeastToHigh = sortedNodesLeastToHigh(allocation, stats);

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
     * @param nodeStats (unused) used for sorting nodes by filesystem usage
     * @return an array of nodes sorted by lowest to highest shard count
     */
    private RoutingNode[] sortedNodesLeastToHigh(RoutingAllocation allocation, final NodesStatsResponse nodeStats) {
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
                logger.trace("comparing " + o1.nodeId() + "[" + c1 + "]"
                        + " to " + o2.nodeId() + "[" + c2 + "]");
                return (c1 - c2);
                //FsStats fs1 = nodeStats.getNodesMap().get(o1.nodeId()).fs();
                //FsStats fs2 = nodeStats.getNodesMap().get(o2.nodeId()).fs();
                //long avgAvailable1 = averageAvailableBytes(fs1);
                //long avgAvailable2 = averageAvailableBytes(fs2);
                //logger.info(avgAvailable1 + " vs. " + avgAvailable2);
                //return (int)(avgAvailable1 - avgAvailable2);
            }
        });
        return nodes;
    }

    /**
     * Averages the available free bytes for a FsStats object
     *
     * @param fs object to average free bytes for
     * @return the average available bytes for all mount points
     */
    private long averageAvailableBytes(FsStats fs) {
        long totalAvail = 0;
        int statNum = 0;
        Iterator<FsStats.Info> i = fs.iterator();
        while (i.hasNext()) {
            FsStats.Info stats = i.next();
            totalAvail += stats.available().bytes();
            statNum++;
        }
        long avg = (totalAvail / statNum);
        logger.info("Average available: " + avg);
        return avg;
    }

    /**
     * Return the FS stats for all nodes, times out if no responses are
     * returned in 10 seconds
     *
     * @return NodesStatsResponse for the FsStats for the cluster
     */
    private NodesStatsResponse nodeFsStats() {
        logger.trace("nodeFsStats");
        NodesStatsRequest request = new NodesStatsRequest(Strings.EMPTY_ARRAY);
        request.timeout(TimeValue.timeValueMillis(10000));
        request.clear();
        request.fs(true);
        NodesStatsResponse resp = nodesStatsAction.execute(request).actionGet(20000);
        return resp;
    }

    /**
     * Check if there is enough disk space for more shards on the node
     *
     * @param shard shard that is being checked
     * @param routingNode node to check disk space of
     * @param nodeStats the NodesStatsResponse for FS stats of the cluster
     * @return true if the node is below the threshold, false if not
     */
    private boolean enoughDiskForShard(MutableShardRouting shard, RoutingNode routingNode, NodesStatsResponse nodeStats) {
        boolean enoughSpace = true;
        
        logger.info("enoughDiskForShard" + shard.shardId() + ", " + routingNode.nodeId());

        FsStats fs = nodeStats.getNodesMap().get(routingNode.nodeId()).fs();
        Iterator<FsStats.Info> i = fs.iterator();
        while (i.hasNext()) {
            FsStats.Info stats = i.next();
            logger.info("Space: " + stats.available().bytes() + " bytes available, " + stats.total().bytes() + " total bytes.");
            double percentFree = ((double)stats.available().bytes() / (double)stats.total().bytes()) * 100.0;
            logger.info("Percentage Free: " + percentFree);
            if (percentFree < this.minimumAvailablePercentage) {
                logger.info("Throttling shard allocation due to excessive disk use " +
                        "(< " + this.minimumAvailablePercentage + "% free)"
                        + " on " + routingNode.nodeId());
                enoughSpace = false;
            }
        }
        return enoughSpace;
    }
}
