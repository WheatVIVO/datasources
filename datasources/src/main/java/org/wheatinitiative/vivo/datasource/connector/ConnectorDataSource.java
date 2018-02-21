package org.wheatinitiative.vivo.datasource.connector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSourceBase;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.ResourceUtils;

public abstract class ConnectorDataSource extends DataSourceBase {
    
    private static final Log log = LogFactory.getLog(ConnectorDataSource.class);
    /* number of iterator elements to be processed at once in memory 
    before being flushed to a SPARQL endpoint */
    protected final static int DEFAULT_BATCH_SIZE = 5;
    
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
        Date currentDateTime = Calendar.getInstance().getTime();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String graphTimeSuffix = "-" + df.format(currentDateTime);
        log.debug("Processing a limit of " + this.getConfiguration().getLimit() + " records");
        log.debug("Processing in batches of " + getBatchSize() + " records");
        String graphURI = getConfiguration().getResultsGraphURI();
        if(!activeEndpointForResults()) {
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
                        addToEndpoint(buffer, graphURI + graphTimeSuffix);
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
        if(activeEndpointForResults()) {
            List<String> allVersionsOfSource = getGraphsWithBaseURI(graphURI, 
                    getSparqlEndpoint());
            for(String version : allVersionsOfSource) {
                if(version.startsWith(graphURI) 
                        && !version.endsWith(graphTimeSuffix)) {
                    log.info("Clearing graph " + version);
                    getSparqlEndpoint().clearGraph(version);
                }
            }
        }
    }
    
    protected List<String> getGraphsWithBaseURI(String baseURI, SparqlEndpoint endpoint) {
        List<String> graphs = new ArrayList<String>();
        String queryStr = "SELECT DISTINCT ?g WHERE { \n" +
                          "    GRAPH ?g { ?s ?p ?o } \n" +
                          "    FILTER(REGEX(STR(?g), \"^" + baseURI + "\")) \n" +
                          "}";
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
    
    private boolean activeEndpointForResults() {
        return (this.getConfiguration().getEndpointParameters() != null);
    }
    
    @Override
    public Model getResult() {
        return this.result;
    }
    
    protected Model renameByIdentifier(Model m, Property identifier, 
            String namespace, String localNamePrefix) {
        Map<Resource, String> idMap = new HashMap<Resource, String>();
        StmtIterator sit = m.listStatements(null, identifier, (RDFNode) null);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            if(stmt.getObject().isLiteral()) {
                idMap.put(stmt.getSubject(), 
                        stmt.getObject().asLiteral().getLexicalForm());
            }
        }
        for(Resource res : idMap.keySet()) {
            ResourceUtils.renameResource(
                    res, namespace + localNamePrefix + idMap.get(res));
        }
        return m;
    }
    
}
