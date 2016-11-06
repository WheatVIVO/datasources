package org.wheatinitiative.vivo.datasource.connector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

public class Rcuk implements Runnable {

    private static final Log log = LogFactory.getLog(Rcuk.class);
    private static final String API_URL = "http://gtr.rcuk.ac.uk/gtr/api/";
    
    private HttpClient httpClient;
    private List<String> queryTerms;
    private String results;
    
    public Rcuk(List<String> queryTerms) {
        this.httpClient = new DefaultHttpClient();
        this.queryTerms = queryTerms;
    }
    
    public void run() {
        List<String> projects = getProjects(queryTerms);
        StringBuffer sb = new StringBuffer();
        for (String project : projects) {
            sb.append(project);
        }
        results = sb.toString();
    }
    
    public List<String> getQueryTerms() {
        return this.queryTerms;
    }
    
    public String getResults() {
        return this.results;
    }
    
    private List<String> getProjects(List<String> queryTerms) {
        List<String> projects = new ArrayList<String>();
        for (String queryTerm : queryTerms) {
            String url = API_URL + "projects?q=" + queryTerm;
            try {
                String response = getApiResponse(url);
                projects.add(response);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return projects;
    }
    
    private String getApiResponse(String url) throws IOException {
        HttpGet get = new HttpGet(url);
        HttpResponse response = httpClient.execute(get);
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
        } finally {
            EntityUtils.consume(response.getEntity());
            if (br != null) {
                br.close();
            }
        }   
        return sb.toString();
    }
}
