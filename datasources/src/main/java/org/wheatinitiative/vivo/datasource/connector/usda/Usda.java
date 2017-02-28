package org.wheatinitiative.vivo.datasource.connector.usda;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import com.hp.hpl.jena.rdf.model.Model;

public class Usda extends VivoDataSource implements DataSource {

    private static final String USDA_VIVO_URL = "http://vivo.usda.gov";
   
    @Override 
    protected String getRemoteVivoURL() {
        return USDA_VIVO_URL;
    }
    
    @Override
    protected Model mapToVIVO(Model model) {
        // USDA VIVO is still using ontology version 1.5
        return updateToOneSix(model);
    }
    
    
}
