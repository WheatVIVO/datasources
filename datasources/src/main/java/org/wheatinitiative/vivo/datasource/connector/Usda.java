package org.wheatinitiative.vivo.datasource.connector;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;

public class Usda extends VivoDataSource implements DataSource {

    private static final String ENDPOINT_URL = "http://vivo.usda.gov/";     
    private Log log = LogFactory.getLog(Usda.class);
    
    public Usda(List<String> filterTerms) {
        super(filterTerms);
    }
    
    @Override
    public void run() {
        try {
            Set<String> uris = new HashSet<String>();
            for (String filterTerm : filterTerms) {
                uris.addAll(getUrisFromSearchResults(ENDPOINT_URL, filterTerm));
            }
        } catch (Exception e) {
            log.error(e, e);
        }
    }

}
