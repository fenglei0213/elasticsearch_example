package com.baidu.fengchao.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESClientBuilder {
    //	private static final String HOST_NAME = "10.94.38.21";
//    private static final String HOST_NAME = "10.46.135.115";
    private static final String HOST_NAME = "10.95.31.20";
    // private static final String CLUSTER_NAME = "single";
    private static final String CLUSTER_NAME = "elasticsearch";

    @SuppressWarnings("resource")
    public static Client buildClient() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", true).put("client", true)
                .put("data", false).put("clusterName", CLUSTER_NAME).build();

        Client client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(HOST_NAME,
                        8300));
        return client;
    }
}
