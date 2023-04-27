package org.wheatinitiative.vivo.datasource;

import org.apache.jena.rdf.model.Model;

public interface DataSource extends Runnable {
    
    public abstract Model getResult();
    
    public abstract DataSourceConfiguration getConfiguration();
    
    public void setConfiguration(DataSourceConfiguration configuration);
    
    public abstract DataSourceStatus getStatus();
    
}
