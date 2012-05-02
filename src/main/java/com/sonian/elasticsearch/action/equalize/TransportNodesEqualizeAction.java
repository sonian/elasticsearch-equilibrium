package com.sonian.elasticsearch.action.equalize;

import com.sonian.elasticsearch.equilibrium.DiskShardsAllocator;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.nodes.*;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author dakrone
 */
public class TransportNodesEqualizeAction extends TransportNodesOperationAction {

    private final DiskShardsAllocator allocator;

    @Inject
    public TransportNodesEqualizeAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                        ClusterService clusterService, TransportService transportService,
                                        DiskShardsAllocator allocator) {
        super(settings, clusterName, threadPool, clusterService, transportService);
        this.allocator = allocator;
    }

    @Override
    protected String transportAction() {
        return "/sonian/rebalance";
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.CACHE;
    }

    @Override
    protected NodesOperationRequest newRequest() {
        return new NodesEqualizeRequest();
    }

    @Override
    protected NodesOperationResponse newResponse(NodesOperationRequest nodesOperationRequest, AtomicReferenceArray nodesResponses) {
        return null;
    }

    @Override
    protected NodeOperationRequest newNodeRequest() {
        return null;
    }

    @Override
    protected NodeOperationRequest newNodeRequest(String nodeId, NodesOperationRequest nodesOperationRequest) {
        return null;
    }

    @Override
    protected NodeOperationResponse newNodeResponse() {
        return null;
    }

    @Override
    protected NodeOperationResponse nodeOperation(NodeOperationRequest nodeOperationRequest) throws ElasticSearchException {
        return null;
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    @Override
    protected void doExecute(ActionRequest actionRequest, ActionListener actionListener) {

    }
}
