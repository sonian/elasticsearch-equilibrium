package com.sonian.elasticsearch.action.equalize;

import com.sonian.elasticsearch.equilibrium.ClusterEqualizerService;
import com.sonian.elasticsearch.equilibrium.DiskShardsAllocator;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * @author dakrone
 */
public class TransportClusterEqualizeAction extends TransportMasterNodeOperationAction<ClusterEqualizeRequest, ClusterEqualizeResponse> {

    private final ClusterEqualizerService equalizeService;

    @Inject
    public TransportClusterEqualizeAction(Settings settings, TransportService transportService,
                                          ClusterService clusterService, ThreadPool threadPool,
                                          ClusterEqualizerService equalizeService) {
        super(settings, transportService, clusterService, threadPool);
        this.equalizeService = equalizeService;
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
    protected ClusterEqualizeRequest newRequest() {
        return new ClusterEqualizeRequest();
    }

    @Override
    protected ClusterEqualizeResponse newResponse() {
        return new ClusterEqualizeResponse();
    }

    @Override
    protected ClusterEqualizeResponse masterOperation(ClusterEqualizeRequest clusterEqualizeRequest, ClusterState state) throws ElasticSearchException {
        return new ClusterEqualizeResponse(this.equalizeService.equalize());
    }
}
