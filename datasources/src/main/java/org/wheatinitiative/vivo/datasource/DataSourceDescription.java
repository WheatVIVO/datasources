package org.wheatinitiative.vivo.datasource;

public class DataSourceDescription {

    private DataSourceConfiguration configuration = null;
    private DataSourceStatus status = null;

    // Simple constructor for Jackson
    public DataSourceDescription() {}
    
    public DataSourceDescription(DataSourceConfiguration configuration, 
            DataSourceStatus status) {
        this.configuration = configuration;
        this.status = status;
    }
    
    public DataSourceConfiguration getConfiguration() {
        return this.configuration;
    }
    
    public void setConfiguration(DataSourceConfiguration configuration) {
        this.configuration = configuration;
    }
    
    public DataSourceStatus getStatus() {
        return this.status;
    }
    
    public void setStatus(DataSourceStatus status) {
        this.status = status;
    }
    
}


