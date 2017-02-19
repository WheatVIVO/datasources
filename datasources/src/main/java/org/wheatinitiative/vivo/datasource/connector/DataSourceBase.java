package org.wheatinitiative.vivo.datasource.connector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSourceConfiguration;
import org.wheatinitiative.vivo.datasource.DataSourceStatus;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

public abstract class DataSourceBase {

    protected boolean stopRequested = false;
    protected RdfUtils rdfUtils;
    protected DataSourceStatus status = new DataSourceStatus();
    protected DataSourceConfiguration configuration = 
            new DataSourceConfiguration();
    protected SparqlEndpoint sparqlEndpoint;
    
    private static final Log log = LogFactory.getLog(DataSourceBase.class);
    
    public DataSourceBase() {
        this.rdfUtils = new RdfUtils();
    }
    
    public DataSourceConfiguration getConfiguration() {
        return this.configuration;
    }
    
    public void setConfiguration(DataSourceConfiguration configuration) {
        this.configuration = configuration;
        this.sparqlEndpoint = getEndpointFromConfiguration(configuration);
    }
    
    public DataSourceStatus getStatus() {
        return this.status;
    }
    
    public void terminate() {
        this.stopRequested = true;
    }
    
    public void run() {
        this.getStatus().setRunning(true);
        try {
            log.info("Running ingest");
            runIngest();  
            log.info("Writing results to endpoint");
            if(this.getConfiguration().getEndpointParameters() != null) {
                writeResultsToEndpoint(getResult());    
            } else {
                log.warn("Not writing results to remote endpoint because " +
                         "none is specified");
            }
        } catch (Exception e) {
            log.info(e, e);
            throw new RuntimeException(e);
        } finally {
            log.info("Finishing ingest");
            log.info(this.getStatus().getErrorRecords() + " errors");
            this.getStatus().setRunning(false);
        }
    }
    
    /**
     * Top level that can be overridden by subclasses
     */
    protected abstract void runIngest();
    
    /**
     * To be overriden by subclasses
     * @return model containing results of ingest
     */
    public abstract Model getResult();
    
    /**
     * Load a named CONSTRUCT query from a file in the SPARQL resources 
     * directory, run it against a model and add the results back to the model.
     * @param queryName name of SPARQL CONSTRUCT query to load
     * @param m model against which query should be executed
     * @param namespaceEtc base string from which URIs should be generated
     * @return model with new data added by CONSTRUCT query
     */
    protected Model construct(String queryName, Model m, String namespaceEtc) {
        m.add(constructQuery(queryName, m, namespaceEtc, null));
        return m;
    }
    
    /**
     * Load a named CONSTRUCT query from a file in the SPARQL resources 
     * directory, run it against a model and return a new model containing
     * only the constructed triples.
     * @param queryName name of SPARQL CONSTRUCT query to load
     * @param m model against which query should be executed
     * @param namespaceEtc base string from which URIs should be generated
     * @return model with triples constructed by query
     */
    protected Model constructQuery(String queryName, Model m, 
            String namespaceEtc, Map<String, String> substitutions) {
        String queryStr = loadQuery(queryName);
        if(substitutions != null) {
            queryStr = processSubstitutions(queryStr, substitutions);
        }
        log.debug(queryStr);
        try {
            QueryExecution qe = QueryExecutionFactory.create(queryStr, m);
            try {
                Model tmp = ModelFactory.createDefaultModel();
                qe.execConstruct(tmp);
                return rdfUtils.renameBNodes(tmp, namespaceEtc, m);
            } finally {
                if(qe != null) {
                    qe.close();
                }
            }
        } catch (QueryParseException qpe) {
            throw new RuntimeException("Error parsing query " + queryName, qpe);
        }
    }
    
    protected String processSubstitutions(String queryStr, 
            Map<String, String> substitutions) {
        for(String old : substitutions.keySet()) {
            // TODO add more sophisticated substitution
            String pattern = old + "\\b";
            queryStr = queryStr.replaceAll(pattern, substitutions.get(old));
        }
        return queryStr;
    }
    
    protected String loadQuery(String resourcePath) {
        InputStream inputStream = this.getClass().getResourceAsStream(
                resourcePath);
        StringBuffer fileContents = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String ln;
            while ( (ln = reader.readLine()) != null) {
                fileContents.append(ln).append('\n');
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to load " + resourcePath, e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return fileContents.toString();
    }
    
    protected SparqlEndpoint getSparqlEndpoint() {
        if(this.sparqlEndpoint != null) {
            return this.sparqlEndpoint;
        } else {
            if(this.getConfiguration() == null 
                    || this.getConfiguration().getEndpointParameters() == null) {
                throw new RuntimeException("Endpoint parameters not specified");
            }
            this.sparqlEndpoint = new SparqlEndpoint(
                    getConfiguration().getEndpointParameters());
            return this.sparqlEndpoint;
        }
    }
    
    protected void writeResultsToEndpoint(Model results) {
        log.info("Writing results to endpoint");
        String graphURI = getConfiguration().getResultsGraphURI();
        if(graphURI == null || graphURI.isEmpty()) {
            throw new RuntimeException("Results graph URI cannot be null or empty");
        }
        log.info("Clearing graph " + graphURI);
        //getSparqlEndpoint().update("CLEAR GRAPH <" + graphURI + ">");
        clearGraph(graphURI);
        log.info("Updating graph " + graphURI);
        getSparqlEndpoint().writeModel(results, graphURI);
    }
    
    protected void addToEndpoint(Model m) {
        String graphURI = getConfiguration().getResultsGraphURI();
        if(graphURI == null || graphURI.isEmpty()) {
            throw new RuntimeException("Results graph URI cannot be null or empty");
        }
        getSparqlEndpoint().writeModel(m, graphURI);
    }
    
    // avoiding CLEAR because it is very inefficient with VIVO-based endpoints
    protected void clearGraph(String graphURI) {
        // retrieve individual URIs in batches of 1000
        int batchSize = 1000;
        log.info("Clearing graph " + graphURI + " in batches of " + batchSize + 
                " individuals");
        boolean getNextBatch = true;
        do {
            String individualsBatch = "SELECT DISTINCT ?s WHERE { \n" +
                    "    GRAPH <" + graphURI + "> { \n" +
                    "        ?s ?p ?o \n" +
                    "    } \n" +
                    "} LIMIT " + batchSize;
            log.debug(individualsBatch);
            ResultSet rs = getSparqlEndpoint().getResultSet(individualsBatch);
            getNextBatch = rs.hasNext();
            StringBuilder deletion = new StringBuilder();
            while(rs.hasNext()) {
                QuerySolution sol = rs.next();
                Resource s = sol.getResource("s");
                if(s.isURIResource()) {
                    deletion.append("DELETE { \n")
                    .append("    GRAPH<").append(graphURI).append(">")
                    .append(" { <").append(s.getURI()).append("> ?p ?o } \n")
                    .append("} WHERE { \n")
                    .append("    GRAPH<").append(graphURI).append(">")
                    .append(" { <").append(s.getURI()).append("> ?p ?o } \n")
                    .append("}; \n");
                }
            }
            String deletionStr = deletion.toString();
            log.debug(deletionStr);
            if(deletionStr.isEmpty()) {
                getNextBatch = false;
            } else {
                getSparqlEndpoint().update(deletion.toString());    
            }            
        } while (getNextBatch);
        // TODO check that count is decreasing after each N batches, otherwise 
        // terminate loop
        // Finally, issue the regular CLEAR to flush out anything remaining
        // (e.g. blank nodes)
        log.info("Clearing graph " + graphURI);
        getSparqlEndpoint().update("CLEAR GRAPH <" + graphURI + ">");
    }
    
    protected SparqlEndpoint getEndpointFromConfiguration(
            DataSourceConfiguration config) {
        if(config == null) {
            throw new RuntimeException(
                    "DataSourceConfiguration cannot be null");
        } else if (config.getEndpointParameters() == null) {
            throw new RuntimeException("Endpoint parameters cannot be null");
        } else if (config.getEndpointParameters()
                .getEndpointURI() == null) {
            throw new RuntimeException("Endpoint URI cannot be null");
        }
        return new SparqlEndpoint(config.getEndpointParameters());
    }
    
}
