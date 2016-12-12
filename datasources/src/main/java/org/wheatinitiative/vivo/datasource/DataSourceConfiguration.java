package org.wheatinitiative.vivo.datasource;

import java.util.HashMap;
import java.util.Map;

public class DataSourceConfiguration {
   
    private String resultsGraphURI;
    private Map<String, Object> parameterMap = new HashMap<String, Object>();
    private SparqlEndpointParams endpointParameters = new SparqlEndpointParams();
    
    public String getResultsGraphURI() {
        return this.resultsGraphURI;
    }
    
    public void setResultsGraphURI(String resultsGraphURI) {
        this.resultsGraphURI = resultsGraphURI;
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
