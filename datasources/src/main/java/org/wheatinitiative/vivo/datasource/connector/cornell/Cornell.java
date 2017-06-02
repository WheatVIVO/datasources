package org.wheatinitiative.vivo.datasource.connector.cornell;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;
import org.wheatinitiative.vivo.datasource.connector.prodinra.Prodinra;

import com.hp.hpl.jena.rdf.model.Model;

public class Cornell extends VivoDataSource implements DataSource {

    private static final String CORNELL_VIVO_URL = "http://vivo.cornell.edu";
    
    private static final String CORNELL_TBOX_NS = "http://cornell.vivo.wheatinitiative.org/";
    private static final String CORNELL_ABOX_NS = CORNELL_TBOX_NS + "individual/";
    private static final String NAMESPACE_ETC = CORNELL_ABOX_NS + "n";
    private static final String SPARQL_RESOURCE_DIR = "/cornell/sparql/";
    

    public static final Log log = LogFactory.getLog(Prodinra.class);
    
   
    @Override 
    protected String getRemoteVivoURL() {
        return CORNELL_VIVO_URL;
    }
    
    
    protected Model constructForVIVO(Model model) {
    	List<String> queries =  Arrays.asList("100-acti-organizations.sparql");
    
    	for(String query : queries) {
    		log.debug("Executing query " + query);
    		log.debug("Pre-query model size: " + model.size());
    		construct(SPARQL_RESOURCE_DIR + query, model, NAMESPACE_ETC);
    		log.debug("Post-query model size: " + model.size());
    	}
         
         return model;
 	}
    
    
    @Override
    protected Model mapToVIVO(Model model) {

    	model = constructForVIVO(model);

    	return model;
    }
    
}
