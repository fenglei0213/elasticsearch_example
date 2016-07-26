package com.baidu.fengchao.client;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Created by fenglei on 2014/8/1.
 */
public class ESClientCluster {

    private static ExecutorService executorService;

    private static Client client = ESClientBuilder.buildClient();

    /**
     * @param client
     * @return
     */
    public static SearchResponse searchIndex(Client client) {
        QueryStringQueryBuilder queryStringBuilder = new QueryStringQueryBuilder(
                "test");
        queryStringBuilder.useDisMax(true);
        queryStringBuilder.field("title", 160);
        queryStringBuilder.field("desc", 1);
        //
        SearchRequestBuilder builder = client.prepareSearch("indexName")
                .setTypes("collectionName").setSearchType(SearchType.DEFAULT)
                .setFrom(0).setSize(1);

        builder.setQuery(queryStringBuilder);
        //
        SearchResponse response = builder.execute().actionGet();
        return response;

    }

    /**
     * @param client
     * @return
     */
    public static GetResponse getData(Client client) {
        GetResponse responseGet = client
                .prepareGet("comment_index", "comment_ugc", "comment_123674")
                .execute().actionGet();
        return responseGet;
    }

    /**
     * @param client
     */
    public static void buildIndex(Client client) {
        IndexResponse response = null;
        response = client
                .prepareIndex("comment_index", "comment_ugc", "comment_123674")
                .setSource(ESClientSingle.buildJsonData()).setTTL(8000).execute()
                .actionGet();
        System.out.println(response.getId());
    }

    /**
     * @param client
     * @param indexList
     */
    public static void buildIndexBatch(Client client,
                                       List<Map<String, Object>> indexList) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Map<String, Object> indexMap : indexList) {
//			bulkRequest.add(client.prepareIndex("comment_index", "comment_ugc",
//					"comment_123674").setSource(indexMap));
            //id自动生成,不要写
            Long userid = (Long) indexMap.get("userid_l");
            Long indexNum = userid % 1024;
            bulkRequest.add(client.prepareIndex("comment_index_" + indexNum, "comment_ugc").setSource(indexMap));
        }
        BulkResponse bulkResponse = null;
        try {
            bulkResponse = bulkRequest.execute().actionGet();
            System.out.println("Threadid " + Thread.currentThread().getId() + " build index ok");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        if (bulkResponse.hasFailures()) {
            // 处理错误
        }
    }

    /**
     * @param user
     * @throws java.io.IOException
     */
    private static void fillWordData(Long user) throws IOException {
        // download userdata:
        String wordUrl = "http://cq01-kafc-data00.vm.baidu.com:8090/browser/index.jsp?sort=1&file=%2Fhome%2Ftq02ksu%2Fworkspace%2Ffcword%2Fsolr%2F"
                + user + ".word";
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(wordUrl);
        CloseableHttpResponse wordRes = httpclient.execute(httpGet);
        try {
            HttpEntity wordEntity = wordRes.getEntity();
            InputStream in = wordEntity.getContent();
            InputStreamReader inr = new InputStreamReader(in, "utf8");
            BufferedReader reader = new BufferedReader(inr);
            List<Map<String, Object>> words = new LinkedList<Map<String, Object>>();
            for (String line = reader.readLine(); line != null; line = reader
                    .readLine()) {
                String[] fields = line.split("\\s*\t\\s*");
                Map<String, Object> model = convertToModel(fields);
                words.add(model);
                if (words.size() == 1000) {
                    fillToES(user, words);
                    words.clear();
                }
            }
            if (words.size() > 0) {
                fillToES(user, words);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            wordRes.close();
            httpclient.close();
        }
    }

    /**
     * @param userid
     * @param words
     * @throws java.io.IOException
     */
    private static void fillToES(long userid, List<Map<String, Object>> words)
            throws IOException {
        ESClientCluster.buildIndexBatch(client, words);
    }

    /**
     * @param fields
     * @return
     */
    public static Map<String, Object> convertToModel(String[] fields) {
        // field name : winfoid, showword, unitid, unitname, planid, planname,
        // userid, wordstat, bid, unitbid, rbid, pcqscore, mqscore, wmatch,
        // wctrl, wmatchprefer
        Map<String, Object> model = new HashMap<String, Object>();
        model.put("id", Long.parseLong(fields[0]));
        model.put("showword_txt", fields[1]);
        model.put("unitid_l", Long.parseLong(fields[2]));
        model.put("unitname_txt", fields[3]);
        model.put("planid_l", Long.parseLong(fields[4]));
        model.put("planname_txt", fields[5]);
        model.put("userid_l", Long.parseLong(fields[6]));
        model.put("wordstat_i", Integer.parseInt(fields[7]));
        model.put("bid_d",
                fields[8].equals("NULL") ? null : Double.parseDouble(fields[8]));
        model.put("unitbid_l", Double.parseDouble(fields[9]));
        model.put("rbid_d", Double.parseDouble(fields[10]));
        model.put("pcqscore_i", Integer.parseInt(fields[11]));
        model.put("mqscore_i", Integer.parseInt(fields[12]));
        model.put("wmatch_i", Integer.parseInt(fields[13]));
        model.put("wctrl_i", Integer.parseInt(fields[14]));
        model.put("wmatchprefer_i", Integer.parseInt(fields[15]));
        return model;
    }

//    public static void main(String[] args) throws Exception {
//        if (args.length > 0 && args[0].matches("\\d+")) {
//            executorService = Executors.newFixedThreadPool(Integer
//                    .parseInt(args[0]));
//        } else {
//            executorService = Executors.newFixedThreadPool(32);
////			executorService = Executors.newSingleThreadExecutor();
//        }
//        List<Long> users = UserUtils.getUsers();
//        for (final Long user : users) {
//            executorService.submit(new Callable<Object>() {
//                public Object call() throws Exception {
//                    fillWordData(user);
//                    return null;
//                }
//            });
//        }
//    }

}
