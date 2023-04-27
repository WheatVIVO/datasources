package org.wheatinitiative.vivo.datasource.connector.florida;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import org.apache.jena.rdf.model.Model;



/**
 * Please note that the "InCommon Certificate Service" that VIVO Florida is using,
 * requires that an intermediate certificate is installed in the client's machine,
 * in order for the authentication process to be successful.
 */
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


    @Override
    protected String getPrefixName() {
        return "ufl";
    }
    
}
