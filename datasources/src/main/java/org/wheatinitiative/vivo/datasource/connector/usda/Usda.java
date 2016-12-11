package org.wheatinitiative.vivo.datasource.connector.usda;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class Usda extends VivoDataSource implements DataSource {

    private static final String ENDPOINT_URL = "http://vivo.usda.gov";
    private static final String PEOPLE = 
            "http://vivoweb.org/ontology#vitroClassGrouppeople";
    private final static int MIN_REST = 300; // ms between linked data requests
    private Log log = LogFactory.getLog(Usda.class);
    
    public Usda(List<String> filterTerms) {
        super(filterTerms);
    }
    
    @Override
    public void run() {
        Model resultModel = ModelFactory.createDefaultModel();
        try {
            Set<String> uris = new HashSet<String>();
            for (String filterTerm : filterTerms) {
                uris.addAll(getUrisFromSearchResults(ENDPOINT_URL, filterTerm, 
                        PEOPLE));
                for(String uri : uris) {
                    
                    //Model m = httpUtils.getRDFLinkedDataResponse(uri);
                    Model m = ModelFactory.createDefaultModel();
                    m.read(uri);
                    resultModel.add(m);
                }
                Thread.sleep(MIN_REST);
            }
        } catch (Exception e) {
            log.error(e, e);
        } finally {
            this.result = resultModel;
        }
    }

}
