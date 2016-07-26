package com.baidu.fengchao.client;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;

/**
 * Created by fenglei on 2014/8/6.
 */
public class ESClientClusterDeleteIndex {
    private static Client client = ESClientBuilder.buildClient();

    public static void deleteIndex(String id) {
        DeleteResponse response = client.prepareDelete("comment_index", "comment_ugc", id)
        .execute().actionGet();

    }

    public static void main(String[] args) {
        String id = "1";
        ESClientClusterDeleteIndex.deleteIndex(id);
    }
}
