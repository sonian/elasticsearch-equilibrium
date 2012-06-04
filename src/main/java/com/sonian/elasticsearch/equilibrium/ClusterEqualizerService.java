package com.sonian.elasticsearch.equilibrium;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;

/**
 * @author dakrone
 */
public class ClusterEqualizerService extends AbstractComponent {

    private final DiskShardsAllocator allocator;
    private final ClusterService clusterService;
    private final AllocationDeciders deciders;

    @Inject
    public ClusterEqualizerService(Settings settings, DiskShardsAllocator allocator, ClusterService clusterService,
                                   AllocationDeciders deciders) {
        super(settings);
        this.allocator = allocator;
        this.clusterService = clusterService;
        this.deciders = deciders;
    }

    public boolean equalize() {
        final AtomicReference<Throwable> failureRef = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(1);
        boolean finished = false;

        clusterService.submitStateUpdateTask("routing-table-updater",new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState clusterState) {
                try {
                    RoutingNodes routingNodes = clusterState.routingNodes();
                    RoutingAllocation allocation = new RoutingAllocation(deciders, routingNodes, clusterState.nodes());

                    if(!allocator.shardSwap(allocation)) {
                        return clusterState;
                    }

                    RoutingAllocation.Result newRouting = new RoutingAllocation.Result(true,
                            new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()),
                            allocation.explanation());

                    return newClusterStateBuilder().state(clusterState).routingResult(newRouting).build();
                } catch (Exception e) {
                    logger.warn("failed to swap shards", e);
                    failureRef.set(e);
                    return clusterState;
                } finally {
                    latch.countDown();
                }
            }
        });

        try {
            finished = latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            failureRef.set(e);
        }

        if (failureRef.get() != null) {
            if (failureRef.get() instanceof ElasticSearchException) {
                throw (ElasticSearchException) failureRef.get();
            } else {
                throw new ElasticSearchException(failureRef.get().getMessage(), failureRef.get());
            }
        }

        return finished;
    }
}
