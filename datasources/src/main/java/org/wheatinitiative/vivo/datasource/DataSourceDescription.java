package org.wheatinitiative.vivo.datasource;

public class DataSourceDescription {

    private DataSourceConfiguration configuration = null;
    private DataSourceStatus status = null;
    private String lastUpdate;
    private String nextUpdate;
    private DataSourceUpdateFrequency updateFrequency;
    private String scheduleAfterURI;
    
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
    
    public String getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(String lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public String getNextUpdate() {
        return nextUpdate;
    }

    public void setNextUpdate(String nextUpdate) {
        this.nextUpdate = nextUpdate;
    }

    public DataSourceUpdateFrequency getUpdateFrequency() {
        return updateFrequency;
    }

    public void setUpdateFrequency(DataSourceUpdateFrequency updateFrequency) {
        this.updateFrequency = updateFrequency;
    }

    public String getScheduleAfterURI() {
        return scheduleAfterURI;
    }

    public void setScheduleAfterURI(String scheduleAfterURI) {
        this.scheduleAfterURI = scheduleAfterURI;
    }

    
}


