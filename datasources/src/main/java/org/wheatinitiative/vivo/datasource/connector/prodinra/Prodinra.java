package org.wheatinitiative.vivo.datasource.connector.prodinra;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.DataSourceBase;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.ResourceUtils;

public class Prodinra extends DataSourceBase implements DataSource {

    public static final Log log = LogFactory.getLog(Prodinra.class);
    
    private static final String METADATA_PREFIX = "oai_inra";
    private static final String PRODINRA_TBOX_NS = "http://record.prodinra.inra.fr/";
    private static final String PRODINRA_ABOX_NS = PRODINRA_TBOX_NS + "individual/";
    private static final String NAMESPACE_ETC = PRODINRA_ABOX_NS + "n";
    private static final String SPARQL_RESOURCE_DIR = "/prodinra/sparql/";
    
    private static final Property RESUMPTION_TOKEN = ResourceFactory.createProperty(
            "http://www.openarchives.org/OAI/2.0//resumptionToken");
    private static final Property TOTAL_RECORDS = ResourceFactory.createProperty(
            "http://ingest.mannlib.cornell.edu/generalizedXMLtoRDF/0.1/completeListSize");
    private static final Property VITRO_VALUE = ResourceFactory.createProperty(
            "http://vitro.mannlib.cornell.edu/ns/vitro/0.7#value");
    
    private Model result;
    
    private HttpUtils httpUtils = new HttpUtils();
    private XmlToRdf xmlToRdf = new XmlToRdf();
    private RdfUtils rdfUtils = new RdfUtils();
    
    @Override
    public void runIngest() {
        try { 
            if(this.getConfiguration().getEndpointParameters() != null) {
                String graphURI = getConfiguration().getResultsGraphURI();
                log.info("Clearing graph " + graphURI);
                clearGraph(graphURI);
            }
            OaiModelIterator it = new OaiModelIterator(
                    this.getConfiguration().getServiceURI(), 
                    METADATA_PREFIX);
            log.info(it.totalRecords() + " total records");
            Model result = ModelFactory.createDefaultModel();
            int page = 0;
            while(it.hasNext()) {
                page++;
                Model model = transformToRDF(it.next());
                log.info(model.size() + " statements before filtering");
                model = filter(model);
                log.info(model.size() + " statements after filtering");
                if(this.getConfiguration().getEndpointParameters() != null) {
                    log.info("Adding " + model.size() + " triples to endpoint");
                    addToEndpoint(model);
                } else {
                    result.add(model);
                }
                log.info(result.size() + " statements after page " + page);
            }            
            this.result = result;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    
    protected Model filter(Model model) {
        Model filtered = ModelFactory.createDefaultModel();
        List<Resource> relevantResources = getRelevantResources(model);
        log.info(relevantResources.size() + " relevant resources");
        for (Resource res : relevantResources) {
            filtered.add(constructPersonalSubgraph(res, model));
        }
        return filtered;
    }
    
    private Model constructPersonalSubgraph(Resource personRes, Model m) {
        Model subgraph = ModelFactory.createDefaultModel();
        Map<String, String> substitutions = new HashMap<String, String>();
        substitutions.put("\\?person", "<" + personRes.getURI() + ">");
        subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getPersonalSubgraph1.sparql", m, 
                NAMESPACE_ETC, substitutions));
        subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getPersonalSubgraph2.sparql", m, 
                NAMESPACE_ETC, substitutions));
        return subgraph;
    }
    
    protected List<Resource> getRelevantResources(Model model) {
        String queryStr = loadQuery(
                SPARQL_RESOURCE_DIR + "getPersonsForSearchTerm.sparql");
        List<Resource> relevantResources = new ArrayList<Resource>();
        for (String queryTerm : getConfiguration().getQueryTerms()) {
            String query = queryStr.replaceAll("\\$TERM", queryTerm);
            log.debug(query);
            QueryExecution qe = QueryExecutionFactory.create(query, model);
            try {
                ResultSet rs = qe.execSelect();
                int count = 0;
                while(rs.hasNext()) {
                    count++;
                    QuerySolution soln = rs.next();
                    Resource res = soln.getResource("person");
                    if(res != null) {
                        relevantResources.add(res);
                    }
                }
                log.info(count + " relevant resources for query term " + queryTerm);
            } finally {
                if(qe != null) {
                    qe.close();
                }
            }
        }
        return relevantResources;
    }
    
    protected Model transformToRDF(Model m) {
        m = rdfUtils.renameBNodes(m, NAMESPACE_ETC, m);
        m = renameByIdentifier(m);
        m = constructForVIVO(m);
        m = rdfUtils.smushResources(m, m.getProperty(
                PRODINRA_TBOX_NS + "identifier"));
        return m;
    }
    
    protected Model renameByIdentifier(Model m) {
        m = renameByIdentifier(m, m.getProperty(
                PRODINRA_TBOX_NS + "identifier"), "id");
        m = renameByIdentifier(m, m.getProperty(
                PRODINRA_TBOX_NS + "inraIdentifier"), "in");
        return m;
    }
    
    private Model renameByIdentifier(Model m, Property identifier, 
            String localNamePrefix) {
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
                    res, PRODINRA_ABOX_NS + localNamePrefix + idMap.get(res));
        }
        return m;
    }
    
    protected Model constructForVIVO(Model m) {
        // TODO dynamically get/sort list from classpath resource directory
        List<String> queries = Arrays.asList("100-documentTypes.sparql",
                "105-title.sparql",
                "102-authorshipPersonTypes.sparql",
                "107-authorLabel.sparql",
                "110-abstract.sparql",
                "112-keywords.sparql",
                "120-externalAffiliation.sparql",
                "122-inraAffiliationUnit.sparql",
                "124-inraLab.sparql");
        for(String query : queries) {
            log.debug("Executing query " + query);
            log.debug("Pre-query model size: " + m.size());
            construct(SPARQL_RESOURCE_DIR + query, m, NAMESPACE_ETC);
            log.debug("Post-query model size: " + m.size());
        }
        return m;
    }
    
    private class OaiModelIterator implements Iterator<Model> {
        
        private URI repositoryURI;
        private String metadataPrefix;
        
        private Model cachedResult = null;
        private Integer totalRecords = null;
        private String resumptionToken = null;
        private boolean done = false;
        
        public OaiModelIterator(String repositoryURL, String metadataPrefix) 
                throws URISyntaxException {
            this.repositoryURI = new URI(repositoryURL);
            this.metadataPrefix = metadataPrefix;
        }

        public boolean hasNext() {
            return (cachedResult != null || !done);
        }

        public Model next() {
            if(cachedResult != null) {
                Model m = cachedResult;
                cachedResult = null;
                return m;
            } else {
                if(done) {
                    throw new RuntimeException("No more items");
                } else {
                    return fetchNext(!CACHE);
                }
            }
        }
        
        private final static boolean CACHE = true;
        
        private void cacheNext() {
            fetchNext(CACHE);
        }
        
        private Model fetchNext(boolean cacheResult) {
            URIBuilder uriB = new URIBuilder(repositoryURI);
            uriB.addParameter("verb", "ListRecords");
            if(resumptionToken != null) {
                uriB.addParameter("resumptionToken", resumptionToken);
            } else {
                uriB.addParameter("metadataPrefix", metadataPrefix);
                //uriB.addParameter("from", "2017-01-01T00:00:00Z");
            }
            try {
                String request = uriB.build().toString();
                log.info(request);
                String response = httpUtils.getHttpResponse(request);
                Model m = xmlToRdf.toRDF(response);
                processResumptionToken(m);
                if(resumptionToken == null) {
                    done = true;
                    log.info("No more resumption token -- done.");
                }
                if(cacheResult) {
                    cachedResult = m;
                }
                return m;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }       
        }

        private void processResumptionToken(Model m) {
            NodeIterator nit = m.listObjectsOfProperty(RESUMPTION_TOKEN);
            String token = null;
            while(nit.hasNext()) {
                RDFNode n = nit.next();
                if(n.isResource()) {
                    if(totalRecords == null) {
                        StmtIterator sit = n.asResource().listProperties(
                                TOTAL_RECORDS);
                        while(sit.hasNext()) {
                            Statement stmt = sit.next();
                            if(stmt.getObject().isLiteral()) {
                                int total = stmt.getObject().asLiteral().getInt();
                                this.totalRecords = new Integer(total);
                            }
                        }
                    } 
                    StmtIterator sit = n.asResource().listProperties(VITRO_VALUE);
                    try {
                        while(sit.hasNext()) {
                            Statement stmt = sit.next();
                            if(stmt.getObject().isLiteral()) {
                                token = stmt.getObject()
                                        .asLiteral().getLexicalForm();
                            }
                        }
                    } finally {
                        sit.close();
                    }
                }
            }
            log.debug("Token: " + token);
            this.resumptionToken = token;
        }
        
        public int totalRecords() {
            if(totalRecords == null) {
                cacheNext();
            }
            return totalRecords;
        }
        
    }

    public Model getResult() {
        return this.result;
    }
   
}
