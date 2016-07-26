package com.baidu.fengchao.client;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;

public class ESClientSearch {
	private static Client client = ESClientBuilder.buildClient();

	/**
	 * 
	 * @param client
	 * @return
	 */
	public static GetResponse getData() {
		GetResponse responseGet = client
				.prepareGet("comment_index", "comment_ugc", "comment_123674")
				.execute().actionGet();
		return responseGet;
	}

	public static void main(String[] args) {
		ESClientSearch.getData();
	}
}
