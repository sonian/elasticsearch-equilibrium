package com.sonian.elasticsearch.rest.action.equilibrium;

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

    @Inject
    public RestNodesEqualizeAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_rebalance", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) {
        try {
            XContentBuilder builder = restContentBuilder(request);

            builder.startObject();
            builder.endObject();

            channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));

        } catch (IOException e) {

        }
    }
}
