package org.wheatinitiative.vivo.datasource.connector;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSourceBase;
import org.wheatinitiative.vivo.datasource.DataSourceConfiguration;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.ResourceUtils;
import com.hp.hpl.jena.vocabulary.RDF;

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
                // TODO this.getStatus().set
                Model model = mapToVIVO(it.next());
                log.debug(model.size() + " statements before filtering");
                model = filter(model);
                log.debug(model.size() + " statements after filtering");
                // TODO
                String defaultNamespace = getDefaultNamespace(this.getConfiguration());
                if(defaultNamespace != null && !(this instanceof VivoDataSource)) {
                    model = rewriteUris(model, defaultNamespace, getPrefixName());
                }
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
    
    private String getDefaultNamespace(DataSourceConfiguration configuration) {
        Object o = configuration.getParameterMap().get("Vitro.defaultNamespace");
        if(o instanceof String) {
            return (String) o;
        } else {
            return null;
        }
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
    
    protected Model rewriteUris(Model model, String namespace, String localNamePrefix) {
        Model out = ModelFactory.createDefaultModel();
        StmtIterator sit = model.listStatements();
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            Resource subj = rewriteResource(stmt.getSubject(), namespace, localNamePrefix);
            RDFNode obj = stmt.getObject();
            if( (!stmt.getPredicate().equals(RDF.type)) && (obj.isURIResource()) ) {
                obj = rewriteResource(obj.asResource(), namespace, localNamePrefix);
            }
            out.add(subj, stmt.getPredicate(), obj);
        }
        return out;
    }
    
    protected Resource rewriteResource(Resource res, String namespace, String localNamePrefix) {
        if(!res.isURIResource()) {
            return res;
        }
        if(res.getURI().startsWith(namespace)) {
            return res;
        }
        try {
            URI uri = new URI(res.getURI());
            String newLocalName = localNamePrefix + uri.getPath();
            newLocalName = newLocalName.replaceAll("/",  "-");
            return ResourceFactory.createResource(namespace + newLocalName);
        } catch (URISyntaxException e) {
            log.debug(e, e);
            return res;
        }                
    }
    
    protected abstract String getPrefixName();
    
}
