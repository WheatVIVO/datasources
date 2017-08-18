package org.wheatinitiative.vivo.datasource.connector.upenn;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import com.hp.hpl.jena.rdf.model.Model;



/**
 * Currently there is an issue with the Certificate Authority that Upenn uses.
 * It seems that "InCommon RSA Server CA" requires the use of an intermediate certificate,
 * in order for the authentication process to be successful.
 */
public class Upenn extends VivoDataSource implements DataSource {

    private static final String UPENN_VIVO_URL = "http://vivo.upenn.edu/vivo";
    
    
    @Override 
    protected String getRemoteVivoURL() {
        return UPENN_VIVO_URL;
    }
    
    
    @Override
    protected Model mapToVIVO(Model model) {
    	// UPENN VIVO is still using ontology version 1.5
        return updateToOneSix(model);
    }
    
}
