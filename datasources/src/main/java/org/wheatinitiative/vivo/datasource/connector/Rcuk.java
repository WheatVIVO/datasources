package org.wheatinitiative.vivo.datasource.connector;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class Rcuk implements Runnable {

    private static final Log log = LogFactory.getLog(Rcuk.class);
    private static final String API_URL = "http://gtr.rcuk.ac.uk/gtr/api/";
    
    private HttpClient httpClient;
    private List<String> queryTerms;
    private Model result;
    
    public Rcuk(List<String> queryTerms) {
        this.httpClient = new DefaultHttpClient();
        this.queryTerms = queryTerms;
    }
    
    public void run() {
        List<String> projects = getProjects(queryTerms);
        StringBuffer sb = new StringBuffer();
        Model m = ModelFactory.createDefaultModel();
        for (String project : projects) {
            m.add(transformToRdf(project));
            sb.append(project);
        }
        result = m;
    }
    
    public List<String> getQueryTerms() {
        return this.queryTerms;
    }
    
    public Model getResult() {
        return this.result;
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
    
    private Model transformToRdf(String doc) {
        InputStream inputStream = new ByteArrayInputStream(doc.getBytes());
        XmlToRdf xmlToRdf = new XmlToRdf();
        return xmlToRdf.toRDF(inputStream);
    }
    
    private String loadQuery(String resourceName) {
        InputStream inputStream = this.getClass().getResourceAsStream(resourceName);
        StringBuffer fileContents = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String ln;
            while ( (ln = reader.readLine()) != null) {
                fileContents.append(ln).append('\n');
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to load " + resourceName, e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return fileContents.toString();
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
