package org.wheatinitiative.vivo.datasource.connector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSourceBase;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public abstract class ConnectorDataSource extends DataSourceBase {
    
    private static final Log log = LogFactory.getLog(ConnectorDataSource.class);
    /* number of iterator elements to be processed at once in memory 
    before being flushed to a SPARQL endpoint */
    protected final static int DEFAULT_BATCH_SIZE = 50;
    
    private Model result;
    
    /**
     * to be overridden by subclasses
     * @return Model representing a discrete "record" in the source data,
     * lifted to RDF but not (necessarily) mapped to the VIVO ontology or
     * filtered for relevance 
     */
    protected abstract IteratorWithSize<Model> getSourceModelIterator();
    
    /**
     * The number of table rows to be processed at once in memory before
     * being flushed to a SPARQL endpoint
     * @return
     */
    protected int getBatchSize() {
        return DEFAULT_BATCH_SIZE;
    }
    
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
        log.debug("Processing a limit of " + this.getConfiguration().getLimit() + " records");
        log.debug("Processing in batches of " + getBatchSize() + " records");
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
        Model buffer = ModelFactory.createDefaultModel();
        int count = 0;
        while(it.hasNext() && count < this.getConfiguration().getLimit()) {
            try {
                count++;
                Model model = mapToVIVO(it.next());
                log.debug(model.size() + " statements before filtering");
                model = filter(model);
                log.debug(model.size() + " statements after filtering");
                if(activeEndpointForResults()) {
                    buffer.add(model);                
                    if(count % getBatchSize() == 0 || !it.hasNext() 
                            || count == this.getConfiguration().getLimit()) {
                        log.debug("Adding " + buffer.size() + " triples to endpoint");
                        addToEndpoint(buffer);
                        buffer.removeAll();
                    }
                } else {
                    result.add(model);
                }
            } catch (Exception e) {
                log.error(e, e);
                this.getStatus().setErrorRecords(this.getStatus().getErrorRecords() + 1);
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
