package org.wheatinitiative.vivo.datasource.connector.concept;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

public class ConceptDataSource extends ConnectorDataSource implements DataSource {

    private static final String SPARQL_RESOURCE_DIR = "/concept/sparql/";
    private static final Log log = LogFactory.getLog(ConceptDataSource.class);
    private Model result = ModelFactory.createDefaultModel();

    @Override
    public void runIngest() {
        log.info("Starting concept processing");
        List<String> queries = Arrays.asList("keywordConceptQuery.sparql");
        getSparqlEndpoint().clearGraph(this.getConfiguration().getResultsGraphURI());
        for(String query : queries) {
            log.info("Executing query " + query);
            Model model = getSparqlEndpoint().construct(loadQuery(SPARQL_RESOURCE_DIR + query));
            log.info("Post-query model size: " + model.size());
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

    @Override
    protected String getPrefixName() {
        return "concept";
    }

}
