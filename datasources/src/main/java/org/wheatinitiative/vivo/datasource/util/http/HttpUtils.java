package org.wheatinitiative.vivo.datasource.util.http;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

public class HttpUtils {

    private HttpClient httpClient = new DefaultHttpClient();
    
    public String getHttpResponse(String url) throws IOException {
        HttpGet get = new HttpGet(url);
        HttpResponse response = httpClient.execute(get);
        try {
            return EntityUtils.toString(response.getEntity());
        } finally {
            EntityUtils.consume(response.getEntity());
        }   
    }
    
    
}
