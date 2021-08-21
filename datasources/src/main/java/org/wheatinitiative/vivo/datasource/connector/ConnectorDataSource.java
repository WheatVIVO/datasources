package org.wheatinitiative.vivo.datasource.connector;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.DataSourceBase;
import org.wheatinitiative.vivo.datasource.DataSourceConfiguration;
import org.wheatinitiative.vivo.datasource.normalizer.AuthorNameForSameAsNormalizer;
import org.wheatinitiative.vivo.datasource.normalizer.LiteratureNameForSameAsNormalizer;
import org.wheatinitiative.vivo.datasource.normalizer.OrganizationNameForSameAsNormalizer;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.indexinginference.IndexingInference;

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
    private static final List<String> FILTER_OUT_GENERIC = Arrays.asList(
            "generalizedXMLtoRDF/0.1", "vitro/0.7#position", "vitro/0.7#value", "XMLSchema-instance");
    private static final String FILTER_OUT_RES = "match_nothing"; 

    protected Model result;

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
    public void runIngest() throws InterruptedException {
        Date currentDateTime = Calendar.getInstance().getTime();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String graphTimeSuffix = "-" + df.format(currentDateTime);
        log.debug("Processing a limit of " + this.getConfiguration().getLimit() + " records");
        log.debug("Processing in batches of " + getBatchSize() + " records");
        String graphURI = getConfiguration().getResultsGraphURI();
        if(!activeEndpointForResults()) {
            result = ModelFactory.createDefaultModel();
        }
        IndexingInference inf = null;
        if(getSparqlEndpoint().getSparqlEndpointParams().getEndpointURI() != null) {
            inf = new IndexingInference(getSparqlEndpoint());    
        }        
        if(inf != null && inf.isAvailable()) {
            inf.unregisterReasoner();
            inf.unregisterSearchIndexer();
            log.info("Unregistered reasoner and search indexer");
        } else {
            log.warn("IndexingInferenceService not available on destination endpoint");
        }
        try {
            IteratorWithSize<Model> it = getSourceModelIterator();
            Integer totalRecords = it.size();
            if(totalRecords != null) {
                this.getStatus().setTotalRecords(totalRecords);
                log.info(it.size() + " total records");
            }        
            Model buffer = ModelFactory.createDefaultModel();
            this.getStatus().setMessage("harvesting records");
            boolean dataWrittenToEndpoint = false;
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
                    String defaultNamespace = getDefaultNamespace(this.getConfiguration());
                    if(defaultNamespace != null && !(this instanceof VivoDataSource)) {
                        model = rewriteUris(model, defaultNamespace, getPrefixName());
                    }
                    if(this.getStatus().isStopRequested()) {
                        throw new InterruptedException();
                    }
                    if(activeEndpointForResults()) {
                        buffer.add(model);                
                        if(count % getBatchSize() == 0 || !it.hasNext() 
                                || count == this.getConfiguration().getLimit()) {
                            if(buffer.size() > 0) {
                                dataWrittenToEndpoint = true;
                            }
                            log.debug("Adding " + buffer.size() + " triples to endpoint");
                            addToEndpoint(buffer, graphURI + graphTimeSuffix);
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
                } catch (Exception e) {
                    log.error(e, e);
                    this.getStatus().setErrorRecords(this.getStatus().getErrorRecords() + 1);
                }
            }
            runNormalizers();
            boolean skipClearingOldData = false;
            if(!dataWrittenToEndpoint) {
                if(totalRecords == null) {
                    skipClearingOldData = true;
                } else if (this.getStatus().getErrorRecords() > (totalRecords / 5)) {
                    skipClearingOldData = true;
                }
            }
            if(activeEndpointForResults() && !skipClearingOldData) {
                this.getStatus().setMessage("removing old data");
                List<String> allVersionsOfSource = getGraphsWithBaseURI(graphURI, 
                        getSparqlEndpoint());
                for(String version : allVersionsOfSource) {
                    if(this.getStatus().isStopRequested()) {
                        throw new InterruptedException();
                    }
                    if(version.startsWith(graphURI) 
                            && !version.endsWith(graphTimeSuffix)) {
                        log.info("Clearing graph " + version);
                        getSparqlEndpoint().clearGraph(version);
                    }
                }
            }
        } finally {
            if(inf != null && inf.isAvailable()) {
                inf.registerReasoner();
                log.info("Registered reasoner");
                inf.registerSearchIndexer();
                log.info("Registered search indexer");
                inf.recompute();
                log.info("Recomputing inferences");
                this.getStatus().setMessage("Recomputing inferences");
                do {
                    Thread.sleep(10000);
                } while (inf.isReasonerIsRecomputing());
                // indexing kicked off after requested inference recompute
                this.getStatus().setMessage("Rebuilding search index");
                do {
                    Thread.sleep(10000);
                } while (inf.isReasonerIsRecomputing()); 
            } else {
                log.warn("IndexingInferenceService not available on destination endpoint");
            }
        }
    }
    
    private void runNormalizers() {
        this.getStatus().setMessage("Running person name normalizer");
        DataSource norm = new AuthorNameForSameAsNormalizer();
        norm.setConfiguration(this.getConfiguration());
        norm.run();
        this.getStatus().setMessage("Running organization name normalizer");
        norm = new OrganizationNameForSameAsNormalizer();
        norm.setConfiguration(this.getConfiguration());
        norm.run();
        this.getStatus().setMessage("Running literature name normalizer");
        norm = new LiteratureNameForSameAsNormalizer();
        norm.setConfiguration(this.getConfiguration());
        norm.run();
    }

    protected String getDefaultNamespace() {
        return getDefaultNamespace(getConfiguration());
    }

    private String getDefaultNamespace(DataSourceConfiguration configuration) {
        Object o = configuration.getParameterMap().get("Vitro.defaultNamespace");
        if(o instanceof String) {
            return (String) o;
        } else {
            return null;
        }
    }

    protected boolean activeEndpointForResults() {
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
        // don't rewrite resources that are already in the default namespace
        if(res.getURI().startsWith(namespace)) {
            return res;
        }
        // don't rewrite things from known ABox namespaces
        if(res.getURI().contains("orcid.org")
                || res.getURI().contains("vivoweb.org")
                || res.getURI().startsWith("http://aims.fao.org/aos/geopolitical")) {
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

    /**
     * A filter that removes raw statements produced by
     * XML (or JSON) to RDF lifting as well as other statements
     * where predicate URIs or rdf:type object URIs contain any of supplied 
     * substrings.
     * @param model
     * @param filterOutSubstrings 
     * @return copy of model containing only statements that were not filtered out
     */
    protected Model filterGeneric(Model model, List<String> filterOutSubstrings) {
        List<String> filterOuts = new ArrayList<String>();
        filterOuts.addAll(FILTER_OUT_GENERIC);
        filterOuts.addAll(filterOutSubstrings);
        Model filtered = ModelFactory.createDefaultModel();
        StmtIterator sit = model.listStatements();
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            String typeURI = null;
            if( (RDF.type.equals(stmt.getPredicate()))
                    && (stmt.getObject().isURIResource()) ) { 
                typeURI = stmt.getObject().asResource().getURI();
            } 
            boolean retainStatement = true;
            for (String filterOut : filterOuts) {
                if( stmt.getPredicate().getURI().contains(filterOut)
                        || (typeURI != null && typeURI.contains(filterOut))) {
                    retainStatement = false;
                    break;
                }
            }
            if(retainStatement) {
                filtered.add(stmt);
            }
        }
        return filtered;
    }

    protected abstract String getPrefixName();

}
