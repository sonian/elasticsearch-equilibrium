package com.sonian.elasticsearch.rest.action.equilibrium;

import com.sonian.elasticsearch.action.equalize.ClusterEqualizeRequest;
import com.sonian.elasticsearch.action.equalize.ClusterEqualizeResponse;
import com.sonian.elasticsearch.action.equalize.TransportClusterEqualizeAction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 * @author dakrone
 */
public class RestNodesEqualizeAction extends BaseRestHandler {

    private final TransportClusterEqualizeAction clusterEqualizeAction;

    @Inject
    public RestNodesEqualizeAction(Settings settings, Client client, RestController controller,
                                   TransportClusterEqualizeAction clusterEqualizeAction) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_rebalance", this);
        this.clusterEqualizeAction = clusterEqualizeAction;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        ClusterEqualizeRequest eqRequest = new ClusterEqualizeRequest();

        clusterEqualizeAction.execute(eqRequest, new ActionListener<ClusterEqualizeResponse>() {
            @Override
            public void onResponse(ClusterEqualizeResponse clusterEqualizeResponse) {
                try {
                    XContentBuilder builder = restContentBuilder(request);

                    builder.startObject();
                    builder.field("success", clusterEqualizeResponse.acknowledged());
                    builder.endObject();

                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));

                } catch (IOException e) { }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });

    }
}
