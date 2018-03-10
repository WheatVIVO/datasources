package org.wheatinitiative.vivo.datasource.connector.tamu;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import com.hp.hpl.jena.rdf.model.Model;



public class Tamu extends VivoDataSource implements DataSource {

    private static final String TAMU_VIVO_URL = "http://scholars.library.tamu.edu/";
   
    
    @Override 
    protected String getRemoteVivoURL() {
        String url =  this.getConfiguration().getServiceURI();
        if(url != null) {
            return url;
        } else {
            return TAMU_VIVO_URL;
        }
    }
    
    
    @Override
    protected Model mapToVIVO(Model model) {
    	// No additional mapping is required.
    	// VIVO Tamu has the latest ontology version.
    	return model;
    }
    
}
