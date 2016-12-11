package org.wheatinitiative.vivo.datasource;

import java.util.HashMap;
import java.util.Map;

public class DataSourceConfiguration {
   
    private Map<String, Object> parameterMap = new HashMap<String, Object>();
    
    public Map<String, Object> getParameterMap() {
        return this.parameterMap;
    }
    
    public void setParameterMap(Map<String, Object> parameterMap) {
        this.parameterMap = parameterMap;
    }
    
}
