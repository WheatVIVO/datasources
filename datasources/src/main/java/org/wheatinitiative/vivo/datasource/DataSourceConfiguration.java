package org.wheatinitiative.vivo.datasource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSourceConfiguration {
   
    private String uri;
    private String name;
    private List<String> queryTerms;
    private String resultsGraphURI;
    private String deploymentURI;
    private int priority;
    private String serviceURI;
    private int limit;
    private int offset;
    private Map<String, Object> parameterMap = new HashMap<String, Object>();
    private SparqlEndpointParams endpointParameters = new SparqlEndpointParams();
    
    public String getURI() {
        return this.uri;
    }
    
    public void setURI(String uri) {
        this.uri = uri;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public List<String> getQueryTerms() {
        return this.queryTerms;
    }
    
    public void setQueryTerms(List<String> queryTerms) {
        this.queryTerms = queryTerms;
    }
    
    public String getResultsGraphURI() {
        return this.resultsGraphURI;
    }
    
    public void setResultsGraphURI(String resultsGraphURI) {
        this.resultsGraphURI = resultsGraphURI;
    }
    
    public String getDeploymentURI() {
        return this.deploymentURI;
    }
    
    public void setDeploymentURI(String deploymentURI) {
        this.deploymentURI = deploymentURI;
    }
    
    public int getPriority() {
        return this.priority;
    }
    
    public void setPriority(int priority) {
        this.priority = priority;
    }
    
    public String getServiceURI() {
        return this.serviceURI;
    }
    
    public void setServiceURI(String serviceURI) {
        this.serviceURI = serviceURI;
    }
    
    public int getLimit() {
        return this.limit;
    }
    
    public void setLimit(int limit) {
        this.limit = limit;
    }
    
    public int getOffset() {
        return this.offset;
    }
    
    public void setOffset(int offset) {
        this.offset = offset;
    }
    
    public SparqlEndpointParams getEndpointParameters() {
        return this.endpointParameters;
    }
    
    public void setEndpointParameters(SparqlEndpointParams endpointParameters) {
        this.endpointParameters = endpointParameters;
    }
    
    public Map<String, Object> getParameterMap() {
        return this.parameterMap;
    }
    
    public void setParameterMap(Map<String, Object> parameterMap) {
        this.parameterMap = parameterMap;
    }
    
}
