package org.wheatinitiative.vivo.datasource.connector;

import org.apache.jena.rdf.model.Model;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.DataSourceBase;

public class GraphClearer extends DataSourceBase implements DataSource {

    @Override
    protected void runIngest() throws InterruptedException {
        getSparqlEndpoint().clearGraph(getConfiguration().getResultsGraphURI());
    }

    @Override
    public Model getResult() {
        return null;
    }
    
}
