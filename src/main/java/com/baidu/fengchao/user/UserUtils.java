package com.baidu.fengchao.user;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by fenglei on 2014/8/1.
 */
public class UserUtils {
    //tq02ksu
//    //asdf1234

    /**
     * @return
     * @throws java.io.IOException
     */
    public static List<Long> getUsers() {
        //后缀以前是.all
        String usersUrl = "http://cq01-kafc-data00.vm.baidu.com:8090/browser/index.jsp?sort=1&file=%2Fhome%2Ftq02ksu%2Fworkspace%2Ffcword%2Fsolr%2Ffc-scorpio-user.part0000.1000w";
//        String usersUrl = "http://cq01-kafc-data00.vm.baidu.com:8090/browser/index.jsp?sort=1&file=%2Fhome%2Ftq02ksu%2Fworkspace%2Ffcword%2Fsolr%2Ffc-scorpio-user.part0000.10";
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(usersUrl);
        CloseableHttpResponse usersRes = null;
        List<Long> users = null;
        try {
            usersRes = httpclient.execute(httpGet);
            HttpEntity usersEntity = usersRes.getEntity();
            InputStream in = usersEntity.getContent();
            List<String> lines = IOUtils.readLines(in);

            users = new LinkedList<Long>();
            for (String l : lines) {
                users.add(Long.parseLong(l.trim()));
            }
        } catch (IOException e) {
        } finally {
            if (usersRes != null) {
                try {
                    usersRes.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return users;
    }
}
