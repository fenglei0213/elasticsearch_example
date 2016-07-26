package com.baidu.fengchao.client;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;

public class ESClientClusterDelete {

    private static Client client = ESClientBuilder.buildClient();

    public static void delete() {
        //index ÊÇdb,type ÊÇtable
        DeleteResponse response = client.prepareDelete("comment_index", "comment_ugc","comment_123674")
                .execute()
                .actionGet();
        System.out.println("delete ok");
    }

    public static void main(String[] args) {
        ESClientClusterDelete.delete();
    }
}
