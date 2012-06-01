package com.sonian.elasticsearch;

import com.sonian.elasticsearch.tests.AbstractJettyHttpServerTests;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.fs.FsStats;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

/**
 * @author dakrone
 */
public class TestUtils extends AbstractJettyHttpServerTests {

    public TestUtils() {}

    // Helpers for tests

    public void start(String id) {
        startNode(id);
    }

    public void createIndex(String id, String name, int numberOfShards, int numberOfRelicas) {
        Client c = client(id);
        c.admin().indices().prepareCreate(name)
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("number_of_shards", numberOfShards)
                        .put("number_of_replicas", numberOfRelicas))
                .execute().actionGet();
    }

    protected void deleteIndex(String id, String name) {
        try {
            client(id).admin().indices().prepareDelete(name).execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
    }

    public ClusterHealthStatus getStatus(String id) {
        Client c = client(id);
        ClusterHealthResponse healthResponse = c.admin().cluster().prepareHealth().setTimeout("2s").execute().actionGet();
        return healthResponse.status();
    }

    public boolean isGreen (String id) {
        return ClusterHealthStatus.GREEN == getStatus(id);
    }

    public boolean isYellow (String id) {
        return ClusterHealthStatus.YELLOW == getStatus(id);
    }

    public boolean isRed (String id) {
        return ClusterHealthStatus.RED == getStatus(id);
    }

    public FsStats makeFakeFsStats(long total, long avail) {
        FsStats fs = createMock(FsStats.class);
        FsStats.Info[] infos = new FsStats.Info[1];

        FsStats.Info fsInfo1 = createMock(FsStats.Info.class);
        expect(fsInfo1.total()).andStubReturn(new ByteSizeValue(total));
        expect(fsInfo1.available()).andStubReturn(new ByteSizeValue(avail));

        infos[0] = fsInfo1;
        expect(fs.iterator()).andStubReturn(Iterators.forArray(infos));

        replay(fs, fsInfo1);

        return fs;
    }
}
