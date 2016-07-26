package com.baidu.fengchao.client;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by fenglei on 2014/8/1.
 */
public class ESClientSingle {
	
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
				.setSource(ESClientSingle.buildJsonData()).setTTL(8000)
				.execute().actionGet();
		System.out.println(response.getId());
	}

	public static void buildIndexBatch(Client client,
			List<Map<String, Object>> indexList) {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (Map<String, Object> indexMap : indexList) {
			for (Map.Entry<String, Object> indexItem : indexMap.entrySet()) {
				bulkRequest.add(client.prepareIndex("comment_index",
						"comment_ugc", "comment_123674").setSource(indexItem));
				bulkRequest.add(client.prepareIndex("comment_index",
						"comment_ugc", "comment_123674").setSource(indexItem));
			}
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			// 处理错误
		}
	}

	/**
	 * for test
	 * 
	 * @return
	 */
	public static String buildJsonData() {
		XContentBuilder xcb = null;
		String text = "";
		try {
			xcb = XContentFactory.jsonBuilder().startObject()
					.field("id", "569874").field("author_name", "riching")
					.field("mark", 232).field("body", "我爱北京天安门")
					.field("createDate", "20130801175520").field("valid", true)
					.endObject();
			// text = JSON.toJSONString(xcb.string());
			text = xcb.string();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// List<Map<String, Object>> words = new LinkedList<Map<String,
		// Object>>();
		// words.add();

		return text;
	}

	public static void main(String[] args) throws Exception {
		ESClientSingle.buildIndex(client);
		Thread.sleep(1000);
		GetResponse responseGet = ESClientSingle.getData(client);
		System.out.println(responseGet.getSourceAsString());
	}

}
