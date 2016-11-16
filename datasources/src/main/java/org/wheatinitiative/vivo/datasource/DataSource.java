package org.wheatinitiative.vivo.datasource;

import com.hp.hpl.jena.rdf.model.Model;

public interface DataSource extends Runnable {
    
    public abstract Model getResult();
    
}
