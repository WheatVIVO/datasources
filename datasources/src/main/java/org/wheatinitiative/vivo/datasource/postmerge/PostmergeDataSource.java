package org.wheatinitiative.vivo.datasource.postmerge;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

public class PostmergeDataSource extends ConnectorDataSource implements DataSource {

    private static final String SPARQL_RESOURCE_DIR = "/postmerge/sparql/";
    private static final String NAMESPACE_ETC = "http://vivo.wheatinitiative.org/individual/postmerge-";
    private static final Log log = LogFactory.getLog(PostmergeDataSource.class);
    private Model result = ModelFactory.createDefaultModel();

    @Override
    public void runIngest() {
        log.info("Starting postmerge processing");
        List<String> queries = Arrays.asList(
                "geoqueries.sparql",
                "participatesIn.sparql",
                "secondTierLocatedIn.sparql",
                "externalToWheatPublicationsQuery.sparql",
                "externalToWheatPublicationsQuery2.sparql",
                "externalToWheatPublicationsQuery3.sparql",
                "externalToWheatGrantsQuery.sparql",
                "externalToWheatProjectsQuery.sparql",
                "externalToWheatPeopleQuery.sparql",
                "externalToWheatPeopleQuery2.sparql",
                "externalToWheatPeopleQuery3.sparql",
                "externalToWheatPeopleQuery4.sparql",
                "externalToWheatOrganizationsQuery.sparql",                
                "externalToWheatJournalsQuery.sparql",
                "externalToWheatConceptsQuery.sparql",
                "externalToWheatInactivePersonsQuery.sparql"
                );
        SparqlEndpoint endpoint = getSparqlEndpoint();
        String resultsGraphURI = this.getConfiguration().getResultsGraphURI();
        endpoint.clearGraph(resultsGraphURI);
        for(String query : queries) {            
            Model model = endpoint.construct(loadQuery(SPARQL_RESOURCE_DIR + query));
            log.info("Postmerge query " + query + " constructed " + model.size());
            endpoint.writeModel(model, resultsGraphURI);
        }
        Model timestamps = endpoint.construct(loadQuery(SPARQL_RESOURCE_DIR + "timestamp.rq"));
        log.info("Writing " + timestamps.size() + " timestamps for newly-ingested individuals");
        endpoint.writeModel(timestamps, "http://vitro.mannlib.cornell.edu/default/vitro-kb-2");
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

    @Override
    protected String getPrefixName() {
        return "postmerge"; 
    }

}
