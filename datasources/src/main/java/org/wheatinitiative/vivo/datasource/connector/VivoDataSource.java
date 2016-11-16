package org.wheatinitiative.vivo.datasource.connector;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;

import com.hp.hpl.jena.rdf.model.Model;

public class VivoDataSource {

    private static final String SEARCH_CONTROLLER = "search";
    private static final String QUERYTEXT_PARAM = "querytext";
    private static final String XML_PARAM = "xml";
    private static final String XML_VALUE = "1";
    
    private Log log = LogFactory.getLog(Usda.class);
    protected List<String> filterTerms;
    protected Model result;
    protected HttpUtils httpUtils = new HttpUtils();
    
    public VivoDataSource(List<String> filterTerms) {
        this.filterTerms = filterTerms;
    }
    
    protected void run() {
        // override
    }

    protected List<String> getUrisFromSearchResults(String vivoUrl, 
            String querytext) throws URISyntaxException, IOException {
        List<String> uris = new ArrayList<String>();
        if(!vivoUrl.endsWith("/")) {
            vivoUrl += "/";
        }
        URIBuilder builder = new URIBuilder(vivoUrl + SEARCH_CONTROLLER);
        builder.addParameter(QUERYTEXT_PARAM, querytext);
        builder.addParameter(XML_PARAM, XML_VALUE);
        URI uri = builder.build();
        String searchResult = httpUtils.getHttpResponse(uri.toString());
        System.out.println(searchResult);
        return uris;
    }
    
    public Model getResult() {
        return this.result;
    }
    
}
