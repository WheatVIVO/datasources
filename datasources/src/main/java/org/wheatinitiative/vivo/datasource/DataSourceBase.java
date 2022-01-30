package org.wheatinitiative.vivo.datasource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;

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
   
    /**
     * Top level to be overridden by subclasses
     */
    protected abstract void runIngest() throws InterruptedException;
    
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
    
    public void run() {
        this.getStatus().setRunning(true);
        try {
            log.info("Running ingest");
            this.getStatus().setMessage("starting ingest");
            this.getStatus().setStatusOk(true);
            this.getStatus().setCompletionPercentage(0);
            this.getStatus().setErrorRecords(0);
            this.getStatus().setProcessedRecords(0);
            this.getStatus().setTotalRecords(0);
            runIngest();  
            if(this.getConfiguration().getEndpointParameters() != null) {
                // Don't clear the graph if the result is empty
                // TODO: should be null instead of empty
                if(getResult() != null && getResult().size() > 0) {
                    writeResultsToEndpoint(getResult());    
                }
            } else {
                log.info("Not writing results to remote endpoint because " +
                         "none is specified");
            }
            this.getStatus().setMessage("ingest complete");
        } catch (InterruptedException e) {
            this.getStatus().setMessage("ingest stopped");
        } catch (Exception e) {
            log.info(e, e);
            this.getStatus().setStatusOk(false);
            this.getStatus().setMessage("ingest terminated due to error " 
                    + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            log.info("Finishing ingest");
            log.info(this.getStatus().getErrorRecords() + " errors");
            this.getStatus().setStopRequested(false);
            this.getStatus().setRunning(false);
        }
    }
    
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
        Model n = constructQuery(queryName, m, namespaceEtc, null);
        log.debug("Query " + queryName + " constructed " + n.size() + " triples.");
        m.add(n);
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
        getSparqlEndpoint().clearGraph(graphURI);
        log.info("Updating graph " + graphURI);
        getSparqlEndpoint().writeModel(results, graphURI);
    }
    
    protected void addToEndpoint(Model m, String graphURI) {
        if(graphURI == null || graphURI.isEmpty()) {
            throw new RuntimeException("Results graph URI cannot be null or empty");
        }
        getSparqlEndpoint().writeModel(m, graphURI);
    }
    
    protected void addToEndpoint(Model m) {
        String graphURI = getConfiguration().getResultsGraphURI();
        if(graphURI == null || graphURI.isEmpty()) {
            throw new RuntimeException("Results graph URI cannot be null or empty");
        }
        getSparqlEndpoint().writeModel(m, graphURI);
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
    
    protected List<String> getGraphsWithBaseURI(String baseURI, SparqlEndpoint endpoint) {
        List<String> graphs = new ArrayList<String>();
        String queryStr = "SELECT DISTINCT ?g WHERE { \n" +
                          "    GRAPH ?g { ?s ?p ?o } \n" +
                          "    FILTER(REGEX(STR(?g), \"^" + baseURI + "\")) \n" +
                          "} ORDER BY ?g";
        ResultSet rs = endpoint.getResultSet(queryStr);
        while(rs.hasNext()) {
            QuerySolution qsoln = rs.next();
            RDFNode n = qsoln.getResource("g");
            if(n.isURIResource()) {
                graphs.add(n.asResource().getURI());
            } else if (n.isLiteral()) { // not supposed to be ...
                graphs.add(n.asLiteral().getLexicalForm());
            }
        }
        return graphs;
    }
    
}
