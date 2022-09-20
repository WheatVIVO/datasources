package org.wheatinitiative.vivo.datasource.connector;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

/**
 * A version of ConnectorDataSource that overrides runIngest so as not to
 * clear the existing source graph nor add a new timestamp suffix to the graph URI
 * @author Brian Lowe
 *
 */
public abstract class InsertOnlyConnectorDataSource extends ConnectorDataSource {

    public static final String LABEL_FOR_SAMEAS = 
            "https://wheatvivo.org/ontology/local/labelForSameAs";
    
    private static final Log log = LogFactory.getLog(InsertOnlyConnectorDataSource.class);    
    
    public String getMostRecentVersion(String graphURI) {
        if(this.getSparqlEndpoint() == null) {
            throw new RuntimeException("SPARQL endpoint is required");
        }
        String resultsGraphURI = this.getConfiguration().getResultsGraphURI();
        if(resultsGraphURI == null || resultsGraphURI.isEmpty()) {
            throw new RuntimeException("results graph URI is required");
        }
        // use the most recent version of the specified graph
        List<String> graphVersions = this.getGraphsWithBaseURI(resultsGraphURI, this.getSparqlEndpoint());
        if(!graphVersions.isEmpty()) {
            String mostRecentVersion = graphVersions.get(graphVersions.size() - 1);
            log.info("Using most recent graph " + mostRecentVersion);
            return mostRecentVersion;
        } else {
            throw new RuntimeException("No graph starting with " + graphURI + " found");
        }
    }
    
    @Override
    public void runIngest() throws InterruptedException {
        log.debug("Processing a limit of " + this.getConfiguration().getLimit() + " records");
        log.debug("Processing in batches of " + getBatchSize() + " records");
        String graphURI = getMostRecentVersion(this.getConfiguration().getResultsGraphURI());
        this.getConfiguration().setResultsGraphURI(graphURI);
        if(!activeEndpointForResults()) {
            result = ModelFactory.createDefaultModel();
        }
        IteratorWithSize<Model> it = getSourceModelIterator();
        Integer totalRecords = it.size();
        if(totalRecords != null) {
            this.getStatus().setTotalRecords(totalRecords);
            log.info(it.size() + " total records");
        }        
        Model buffer = ModelFactory.createDefaultModel();
        this.getStatus().setMessage("harvesting records");
        int count = 0;
        while(it.hasNext() && count < this.getConfiguration().getLimit()) {
            try {
                if(this.getStatus().isStopRequested()) {
                    throw new InterruptedException();
                }
                count++;                
                Model model = mapToVIVO(it.next());
                log.debug(model.size() + " statements before filtering");
                if(this.getStatus().isStopRequested()) {
                    throw new InterruptedException();
                }
                model = filter(model);
                log.debug(model.size() + " statements after filtering");
                if(this.getStatus().isStopRequested()) {
                    throw new InterruptedException();
                }
                if(this.getStatus().isStopRequested()) {
                    throw new InterruptedException();
                }
                if(activeEndpointForResults()) {
                    buffer.add(model);                
                    if(count % getBatchSize() == 0 || !it.hasNext() 
                            || count == this.getConfiguration().getLimit()) {
                        log.debug("Adding " + buffer.size() + " triples to endpoint");
                        addToEndpoint(buffer, graphURI);
                        buffer.removeAll();
                    }
                } else {
                    result.add(model);
                }
                this.getStatus().setProcessedRecords(count);                
                if(totalRecords != null && totalRecords > 0) {
                    float completionPercentage = ((float) count / (float) totalRecords) * 100;
                    log.info("Completion percentage " + completionPercentage);
                    this.getStatus().setCompletionPercentage((int) completionPercentage);
                }
            } catch (InterruptedException e) {
                throw(e); // this is the one exception we want to throw 
            } catch (Throwable t) {
                log.error(t, t);
                this.getStatus().setErrorRecords(this.getStatus().getErrorRecords() + 1);
            }
        }
    }
    
    protected void clearPreviousResults(String propertyURI, SparqlEndpoint endpoint, String graphURI) {
        log.info("Removing " + propertyURI + " triples from graph " + graphURI);
        String constructQueryStr = "CONSTRUCT { ?s <" + propertyURI + "> ?o } WHERE { \n" 
                + "  GRAPH <" + graphURI + "> { ?s <" + propertyURI + "> ?o } \n" 
                + "} \n";
        Model toRemove = endpoint.construct(constructQueryStr);
        log.info("Removing " + toRemove.size() + " triples");
        endpoint.deleteModel(toRemove, graphURI);        
    }
   
}
