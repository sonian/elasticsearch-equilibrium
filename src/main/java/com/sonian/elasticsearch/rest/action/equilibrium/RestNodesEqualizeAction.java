package com.sonian.elasticsearch.rest.action.equilibrium;

import com.sonian.elasticsearch.action.equalize.NodesEqualizeRequest;
import com.sonian.elasticsearch.action.equalize.NodesEqualizeResponse;
import com.sonian.elasticsearch.action.equalize.TransportNodesEqualizeAction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 * @author dakrone
 */
public class RestNodesEqualizeAction extends BaseRestHandler {

    private final TransportNodesEqualizeAction nodesEqualizeAction;

    @Inject
    public RestNodesEqualizeAction(Settings settings, Client client, RestController controller,
                                   TransportNodesEqualizeAction nodesEqualizeAction) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_rebalance", this);
        this.nodesEqualizeAction = nodesEqualizeAction;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        NodesEqualizeRequest eqRequest = new NodesEqualizeRequest();

        nodesEqualizeAction.execute(eqRequest, new ActionListener<NodesEqualizeResponse>() {
            @Override
            public void onResponse(NodesEqualizeResponse nodesEqualizeResponse) {
                try {
                    XContentBuilder builder = restContentBuilder(request);

                    builder.startObject();
                    builder.endObject();

                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));

                } catch (IOException e) {

                }
            }

            @Override
            public void onFailure(Throwable e) {

            }
        });

    }
}
