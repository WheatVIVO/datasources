package org.wheatinitiative.vivo.datasource.connector.tamu;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import com.hp.hpl.jena.rdf.model.Model;

public class Tamu extends VivoDataSource implements DataSource {

    private static final String TAMU_VIVO_URL = "http://scholars.library.tamu.edu/vivo";
   
    @Override 
    protected String getRemoteVivoURL() {
        return TAMU_VIVO_URL;
    }
    
    @Override
    protected Model mapToVIVO(Model model) {
    	return model;
    }
    
}