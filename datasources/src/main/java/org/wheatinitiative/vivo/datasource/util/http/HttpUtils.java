package org.wheatinitiative.vivo.datasource.util.http;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.util.EntityUtils;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class HttpUtils {

    private HttpClient httpClient;
    
    public HttpUtils() {
        DefaultHttpClient defaultHttpClient = new DefaultHttpClient();
        defaultHttpClient.setRedirectStrategy(new LaxRedirectStrategy());
        this.httpClient = defaultHttpClient;
    }
    
    public String getHttpResponse(String url) throws IOException {
        HttpGet get = new HttpGet(url);
        HttpResponse response = httpClient.execute(get);
        try {
            return EntityUtils.toString(response.getEntity());
        } finally {
            EntityUtils.consume(response.getEntity());
        }   
    }
    
    public String getHttpPostResponse(String url, String payload, 
            String contentType) {
        HttpPost post = new HttpPost(url);
        post.addHeader("content-type", contentType);
        try {
            post.setEntity(new StringEntity(payload));
            try {
                HttpResponse response = httpClient.execute(post);
                try {
                    return EntityUtils.toString(response.getEntity());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    EntityUtils.consume(response.getEntity());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } 
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    public Model getRDFLinkedDataResponse(String url) throws IOException {
        HttpGet get = new HttpGet(url);
        get.setHeader("Accept", "application/rdf+xml");
        HttpResponse response = httpClient.execute(get);
        try {
            byte[] entity = EntityUtils.toByteArray(response.getEntity());
            Model model = ModelFactory.createDefaultModel();
            model.read(new ByteArrayInputStream(entity), null, "RDF/XML");
            return model;
        } finally {
            EntityUtils.consume(response.getEntity());
        }
    }
    
    
}
