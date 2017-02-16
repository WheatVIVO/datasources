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
        if(this.configuration == null) {
            this.configuration = new DataSourceConfiguration();
        }
        return this.configuration;
    }
    
    public void setConfiguration(DataSourceConfiguration configuration) {
        this.configuration = configuration;
    }
    
    public DataSourceStatus getStatus() {
        if(this.status == null) {
            this.status = new DataSourceStatus();
        }
        return this.status;
    }
    
    public void setStatus(DataSourceStatus status) {
        this.status = status;
    }
    
}


