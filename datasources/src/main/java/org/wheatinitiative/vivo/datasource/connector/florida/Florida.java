package org.wheatinitiative.vivo.datasource.connector.florida;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import com.hp.hpl.jena.rdf.model.Model;

public class Florida extends VivoDataSource implements DataSource {
	
    private static final String FLORIDA_VIVO_URL = "http://vivo.ufl.edu";
    
    @Override 
    protected String getRemoteVivoURL() {
        return FLORIDA_VIVO_URL;
    }
    
    @Override
    protected Model mapToVIVO(Model model) {
        return model;
    }
    
}
