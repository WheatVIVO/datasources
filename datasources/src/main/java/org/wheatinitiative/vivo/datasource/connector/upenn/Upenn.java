package org.wheatinitiative.vivo.datasource.connector.upenn;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import com.hp.hpl.jena.rdf.model.Model;



/**
 * Please note that the "InCommon Certificate Service" that VIVO Upenn is using,
 * requires that an intermediate certificate is installed in the client's machine,
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


    @Override
    protected String getPrefixName() {
        return "upenn";
    }
    
}
