package org.wheatinitiative.vivo.datasource.util.http;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
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
    private static final Log log = LogFactory.getLog(HttpUtils.class);
    
    public HttpUtils() {
        DefaultHttpClient defaultHttpClient = new DefaultHttpClient();
        defaultHttpClient.setRedirectStrategy(new LaxRedirectStrategy());
        this.httpClient = defaultHttpClient;
    }
    
    public String getHttpResponse(String url) throws IOException {
        HttpGet get = new HttpGet(url);
        HttpResponse response;
        try {
            response = httpClient.execute(get);
        } catch (Exception e) {
            try {
                Thread.sleep(2000);
                response = httpClient.execute(get);
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            } catch (Exception e2) {
                try {
                    Thread.sleep(4000);
                    response = httpClient.execute(get);
                } catch (InterruptedException e3) {
                    throw new RuntimeException(e3);
                }
            }
        }
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
