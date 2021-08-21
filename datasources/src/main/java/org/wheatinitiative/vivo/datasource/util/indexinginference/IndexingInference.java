package org.wheatinitiative.vivo.datasource.util.indexinginference;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

/**
 * Provides access to the IndexingInferenceService of an appropriately-extended
 * VIVO instance
 *
 */
public class IndexingInference {

    private HttpUtils httpUtils = new HttpUtils();
    private String serviceURI;
    private static final Log log = LogFactory.getLog(IndexingInference.class);
    
    /**
     * Construct an IndexingInference instance for an IndexingInferenceService
     * at the specified URL
     * @throws IllegalArgumentException if serviceURI is null
     */
    public IndexingInference(String serviceURI) {
        if(serviceURI == null) {
            throw new IllegalArgumentException("serviceURI may not be null");
        }
        this.serviceURI = serviceURI;
    }
    
    /**
     * Construct an IndexingInference instance from a VIVO SparqlEndpoint
     * @param sparqlEndpoint
     * @throws IllegalArgumentException if sparqlEndpoint is null its enpoint
     * URI is null, or the endpoint URI does not end with /api/sparqlQuery
     */
    public IndexingInference(SparqlEndpoint sparqlEndpoint) {
        if(sparqlEndpoint == null) {
            throw new IllegalArgumentException("sparqlEndpoint may not be null");
        }
        if(sparqlEndpoint.getSparqlEndpointParams() == null
                || sparqlEndpoint.getSparqlEndpointParams().getEndpointURI() == null) {
            throw new IllegalArgumentException("sparqlEndpoint's endpointURI may not be null");
        }
        if(!sparqlEndpoint.getSparqlEndpointParams().getEndpointURI().endsWith("/api/sparqlQuery")) {
            throw new IllegalArgumentException("sparqlEndpoint is not a VIVO"
                    + " instance (does not end with /api/sparqlQuery)");
        }
        String endpointURI = sparqlEndpoint.getSparqlEndpointParams().getEndpointURI();
        this.serviceURI = endpointURI.substring(0, endpointURI.length()
                - "/api/sparqlQuery".length()) + "/vds/indexingInference";
        log.info("Using IndexingInferenceService at " + serviceURI);
    }
    
    public boolean isAvailable() {
        try {
            String response = httpUtils.getHttpResponse(serviceURI + "/status");
            JSONObject json = new JSONObject(response);
            return json.has("reasonerRegisteredForChanges");
        } catch (IOException e) {
            log.error(e, e);
            return false;
        } catch (Exception e) {
            log.debug(e, e);
            return false;
        }
    }
      
    private boolean getStatusProperty(String propertyName) {
        try {
            String response = httpUtils.getHttpResponse(serviceURI + "/status");
            JSONObject json = new JSONObject(response);
            return json.getBoolean(propertyName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public boolean isSearchIndexerIsIndexing() {
        return getStatusProperty("searchIndexerIsIndexing");
    }
    
    public boolean isReasonerRegisteredForChanges() {
        return getStatusProperty("reasonerRegisteredForChanges");
    }
    
    public boolean isSearchIndexerRegisteredForChanges() {
        return getStatusProperty("searchIndexerRegisteredForChanges");
    }
    
    public boolean isReasonerIsRecomputing() {
        return getStatusProperty("reasonerIsRecomputing");
    }
    
    public void registerReasoner() {
        httpUtils.getHttpPostResponse(serviceURI + "/reasoner/register", "", "application/json");
    }
    
    public void unregisterReasoner() {
        httpUtils.getHttpPostResponse(serviceURI + "/reasoner/unregister", "", "application/json");
    }
    
    public void registerSearchIndexer() {
        httpUtils.getHttpPostResponse(serviceURI + "/searchIndexer/register", "", "application/json");
    }
    
    public void unregisterSearchIndexer() {
        httpUtils.getHttpPostResponse(serviceURI + "/searchIndexer/unregister", "", "application/json");
    }
    
    public void recompute() {
        httpUtils.getHttpPostResponse(serviceURI + "/reasoner/recompute", "", "application/json");
    }
    
    public void index() {
        httpUtils.getHttpPostResponse(serviceURI + "/searchIndexer/index", "", "application/json");
    }
}
