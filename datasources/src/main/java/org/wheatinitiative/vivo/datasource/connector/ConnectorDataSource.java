package org.wheatinitiative.vivo.datasource.connector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSourceBase;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public abstract class ConnectorDataSource extends DataSourceBase {
    
    private static final Log log = LogFactory.getLog(ConnectorDataSource.class);
    private Model result;
    /**
     * to be overridden by subclasses
     * @return Model representing a discrete "record" in the source data,
     * lifted to RDF but not (necessarily) mapped to the VIVO ontology or
     * filtered for relevance 
     */
    protected abstract IteratorWithSize<Model> getSourceModelIterator();
    
    /**
     * to be overridden by subclasses
     * @param model
     * @return model filtered to relevant resources according to the query
     * terms or other criteria
     */
    protected abstract Model filter(Model model);
    
    /**
     * to be overridden by subclasses
     * @param model
     * @return model with VIVO-compatible statements added
     */
    protected abstract Model mapToVIVO(Model model);
 
    @Override
    public void runIngest() {
        if(activeEndpointForResults()) {
            String graphURI = getConfiguration().getResultsGraphURI();
            log.info("Clearing graph " + graphURI);
            getSparqlEndpoint().clearGraph(graphURI);
        } else {
            result = ModelFactory.createDefaultModel();
        }
        IteratorWithSize<Model> it = getSourceModelIterator();
        if(it.size() != null) {
            log.info(it.size() + " total records");
        }        
        int count = 0;
        while(it.hasNext() && count < this.getConfiguration().getLimit()) {
            count++;
            Model model = mapToVIVO(it.next());
            log.debug(model.size() + " statements before filtering");
            model = filter(model);
            log.debug(model.size() + " statements after filtering");
            if(activeEndpointForResults()) {
                log.debug("Adding " + model.size() + " triples to endpoint");
                addToEndpoint(model);
            } else {
                result.add(model);
            }
        }
    }
    
    private boolean activeEndpointForResults() {
        return (this.getConfiguration().getEndpointParameters() != null);
    }
    
    @Override
    public Model getResult() {
        return this.result;
    }
    
}
