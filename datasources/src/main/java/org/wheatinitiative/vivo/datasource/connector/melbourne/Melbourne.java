package org.wheatinitiative.vivo.datasource.connector.melbourne;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import com.hp.hpl.jena.rdf.model.Model;

public class Melbourne extends VivoDataSource implements DataSource {
	
    private static final String MELBOURNE_VIVO_URL = "http://findanexpert.unimelb.edu.au";
    
    @Override 
    protected String getRemoteVivoURL() {
        return MELBOURNE_VIVO_URL;
    }
    
    @Override
    protected Model mapToVIVO(Model model) {
        // MELBOURNE VIVO is still using ontology version 1.4
    	// Hoping that the current "updateToOneSix(Model)" method will update most of the data.
        return updateToOneSix(model);
    }
    
    /*
     * Melbourne's requests return some HTTP:404s, upper level code (VivoDatasource) should handle it.
     */
    
}
