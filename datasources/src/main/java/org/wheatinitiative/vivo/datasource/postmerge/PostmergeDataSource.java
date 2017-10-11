package org.wheatinitiative.vivo.datasource.postmerge;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class PostmergeDataSource extends ConnectorDataSource implements DataSource {

    private static final String SPARQL_RESOURCE_DIR = "/postmerge/sparql/";
    private static final String NAMESPACE_ETC = "http://vivo.wheatinitiative.org/individual/postmerge-";
    private static final Log log = LogFactory.getLog(PostmergeDataSource.class);
    private Model result = ModelFactory.createDefaultModel();

    @Override
    public void runIngest() {
        log.info("Starting postmerge processing");
        getSparqlEndpoint();
        List<String> queries = Arrays.asList(
                "geoqueries.sparql",
                "participatesIn.sparql",
                "secondTierLocatedIn.sparql",
                "externalToWheatPublicationsQuery.sparql",
                "externalToWheatPeopleQuery.sparql",
                "externalToWheatOrganizationsQuery.sparql"
                );
        getSparqlEndpoint().clearGraph(this.getConfiguration().getResultsGraphURI());
        for(String query : queries) {
            Model model = ModelFactory.createDefaultModel();
            log.debug("Executing query " + query);
            log.debug("Pre-query model size: " + model.size());
            construct(SPARQL_RESOURCE_DIR + query, model, NAMESPACE_ETC);
            log.debug("Post-query model size: " + model.size());
            getSparqlEndpoint().writeModel(model, this.getConfiguration().getResultsGraphURI());
        }
    }

    @Override
    public Model getResult() {
        return this.result;
    }

    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        return null;
    }

    @Override
    protected Model filter(Model model) {
        return model;
    }

    @Override
    protected Model mapToVIVO(Model model) {
        return model;
    }

}
