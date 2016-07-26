package com.baidu.fengchao.client;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders.*;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryBuilders.*;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class ESClientClusterQuery {
    private static Client client = ESClientBuilder.buildClient();

    public static void query() {
        System.out.println("query ok");
//        TermQueryBuilder query = QueryBuilders.termQuery("name", "kimchy");
        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();
        SearchResponse response = client
               .prepareSearch("comment_index")
                .setQuery(query)
//                .setFrom(0).setSize(60).setExplain(true)
                .execute().actionGet();
        System.out.println("hits: " + response.getHits().getTotalHits());
    }

    public static void main(String[] args) {
//        for(int i=0;i<2;i++) {
//            ESClientClusterQuery.query();
//            try {
//                Thread.sleep(1000*60*1);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
        Timestamp ts = new Timestamp(1407990936590l);
        String tsStr = "";
        DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        try {

            tsStr = ts.toString();
            System.out.println(tsStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
