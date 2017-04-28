package org.wheatinitiative.vivo.datasource.connector.cordis;

// Java imports
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Apache imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;

//Wheatinitiative imports
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

// Jena imports
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.ResourceUtils;



public class Cordis extends ConnectorDataSource implements DataSource {
	
    public static final Log log = LogFactory.getLog(Cordis.class);

    private static final String CORDIS_TBOX_NS = "http://cordis.europa.eu/";
    private static final String CORDIS_ABOX_NS = CORDIS_TBOX_NS + "individual/";
    private static final String NAMESPACE_ETC = CORDIS_ABOX_NS + "n";
    private static final String SPARQL_RESOURCE_DIR = "/cordis/sparql/";
    private static final int MIN_REST_AFTER_HTTP_REQUEST = 250; //ms

    
    
    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        return new CordisModelIterator(
                this.getConfiguration().getServiceURI(), 
                this.getConfiguration().getQueryTerms()
                );
    }
    
	
	private class CordisModelIterator implements IteratorWithSize<Model> {

        private String service_URI;
        private List<String> queryTerms;
        private static final int SEARCH_RESULTS_PER_PAGE = 20;

        private boolean done = false;
        private HttpUtils httpUtils = new HttpUtils();
        private XmlToRdf xmlToRdf = new XmlToRdf();
        private RdfUtils rdfUtils = new RdfUtils();
        
        private Map<String, Model> initialResultsCache;
        private Map<String, Integer> totalForQueryTerm;
        private Iterator<String> queryTermIterator;
        private String currentQueryTerm;
        private int currentResultForQueryTerm = 1;
	    
		
	    // Constructor
        public CordisModelIterator(String serviceURI, List<String> queryTerms) {
            this.service_URI = serviceURI;
            this.queryTerms = queryTerms;
            this.queryTermIterator = queryTerms.iterator();
            initialResultsCache = fetchInitialResults();
            totalForQueryTerm = getTotalsForQueryTerms(initialResultsCache);
        }
		
        public boolean hasNext() {
            return ((this.size() > 0) && !done);
        }
        
        public Model next() {
            if(currentQueryTerm == null){
                if(queryTermIterator.hasNext()) {
                    currentQueryTerm = queryTermIterator.next();
                    currentResultForQueryTerm = 1;
                } else {
                    done = true;
                    return ModelFactory.createDefaultModel();
                }
            }
            Model searchResultsModel = null;
            if(currentResultForQueryTerm == 1) {
                searchResultsModel = initialResultsCache.get(currentQueryTerm);
            } else {
                searchResultsModel = fetchResults(
                        currentQueryTerm, currentResultForQueryTerm);
            }
            currentResultForQueryTerm += SEARCH_RESULTS_PER_PAGE;
            if(currentResultForQueryTerm > totalForQueryTerm.get(currentQueryTerm)) {
                if(queryTermIterator.hasNext()) {
                    currentQueryTerm = queryTermIterator.next();	// Go to the next query term
                    currentResultForQueryTerm = 1;
                } else {
                    done = true;
                }
            }             
            return getResourcesForSearchResults(searchResultsModel);
        }

        
        public Integer size() {
            return totalResults(initialResultsCache);
        }
        
        
        private Map<String, Model> fetchInitialResults() {
            Map<String, Model> initialResultsCache = new HashMap<String, Model>();
            for (String queryTerm : queryTerms) {
                initialResultsCache.put(queryTerm, fetchResults(queryTerm, 1));
            }
            return initialResultsCache;
        }
        
        
        private Map<String, Integer> getTotalsForQueryTerms(
                Map<String, Model> initialResultsCache) {
            Map<String, Integer> totalsForQueryTerms = new HashMap<String, Integer>();
            for(String queryTerm : initialResultsCache.keySet()) {
                int total = totalResults(initialResultsCache.get(queryTerm));
                totalsForQueryTerms.put(queryTerm, total);
                log.info(total + " results for query term " + queryTerm);
            }
            return totalsForQueryTerms;
        }
        
        
        private Model fetchResults(String queryTerm, int startingResult) {
            try {
                URIBuilder uriB = new URIBuilder(this.service_URI);
                uriB.addParameter("q", queryTerm);
                uriB.addParameter("format", "xml");
                uriB.addParameter("p", Integer.toString(
                        (startingResult / SEARCH_RESULTS_PER_PAGE) + 1, 10));
                uriB.addParameter("num", Integer.toString(
                        SEARCH_RESULTS_PER_PAGE, 10));
                String request = uriB.build().toString();
                log.info("Requesting search result page " + request);
                String response = httpUtils.getHttpResponse(request);
                Model model = xmlToRdf.toRDF(response);
                Thread.sleep(MIN_REST_AFTER_HTTP_REQUEST);
                return rdfUtils.renameBNodes(model, NAMESPACE_ETC, model);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        
        
        private Model getResourcesForSearchResults(Model searchResultsModel) {
            Model resourcesModel = ModelFactory.createDefaultModel();
            Collection<String> resultURIs = getResultURIs(searchResultsModel);
            for(String resultURI : resultURIs) {
                try {
                    log.info("Requesting result " + resultURI);
                    String response = httpUtils.getHttpResponse(resultURI);
                    resourcesModel.add(xmlToRdf.toRDF(response));
                    Thread.sleep(MIN_REST_AFTER_HTTP_REQUEST);                                        
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return rdfUtils.renameBNodes(
                    resourcesModel, NAMESPACE_ETC, resourcesModel);
        }
        
        
        /*
         * get total hits for a map of query term to first search result model
         */
        private int totalResults(Map<String, Model> initialResultsCache) {
            int total = 0;
            for(String queryTerm : initialResultsCache.keySet()) {
                total += totalResults(initialResultsCache.get(queryTerm));
            }
            return total;
        }
        
        
        /*
         * get total hits for a model representing the first results page 
         * of a search for a given query term 
         */
        private int totalResults(Model m) {
            return getIntValue(CORDIS_TBOX_NS + "totalHits", m);
        }
        
        
        private int getIntValue(String predicateURI, Model m) {
            StmtIterator sit = m.listStatements(
                    null, m.getProperty(predicateURI), (RDFNode) null);
            try {
                while(sit.hasNext()) {
                    Statement stmt = sit.next();
                    if(stmt.getObject().isLiteral()) {
                        try {
                            return stmt.getObject().asLiteral().getInt();
                        } catch (Exception e) {
                            // not an int, apparently
                        }
                    }
                }
            } finally {
                sit.close();
            }
            return -1;
        }
        
        
        private Collection<String> getResultURIs(Model searchResultsModel) {
            List<String> resultURIs = new ArrayList<String>();
            resultURIs.addAll(getResultURIs(searchResultsModel, "result"));
            resultURIs.addAll(getResultURIs(searchResultsModel, "project"));
            return resultURIs;
        }
        
        
        private Collection<String> getResultURIs(Model searchResultsModel, String type) {
            // use set because some values are repeated (e.g. projects)
            Set<String> resultURIs = new HashSet<String>();
            Property rcn = searchResultsModel.getProperty(CORDIS_TBOX_NS + "rcn");
            NodeIterator nit = searchResultsModel.listObjectsOfProperty(
                    searchResultsModel.getProperty(CORDIS_TBOX_NS + type));
            while(nit.hasNext()) {
                RDFNode node = nit.next();
                if(!node.isResource()) {
                    continue;
                }
                StmtIterator sit = node.asResource().listProperties(rcn);
                while(sit.hasNext()) {
                    Statement stmt = sit.next();
                    if(stmt.getObject().isLiteral()) {
                        String rcnValue = stmt.getObject().asLiteral().getLexicalForm();
                        resultURIs.add(makeResultURI(rcnValue, type));
                    }
                }
            }
            return resultURIs;
        }
        
        
        private String makeResultURI(String rcnValue, String type) {
            return CORDIS_TBOX_NS + type + "/rcn/" + rcnValue + "_en.xml";  
        }
    }
	
	
	protected Model renameByIdentifier(Model model, Property identifier, 
            String localNamePrefix) {
		
		 Map<Resource, String> idMap = new HashMap<Resource, String>();
        StmtIterator sit = model.listStatements(null, identifier, (RDFNode) null);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            if(stmt.getObject().isLiteral()) {
                idMap.put(stmt.getSubject(), 
                        stmt.getObject().asLiteral().getLexicalForm());
            }
        }
        for(Resource res : idMap.keySet()) {
            ResourceUtils.renameResource(
                    res, CORDIS_ABOX_NS + localNamePrefix + idMap.get(res));
        }
		
		return model;
	}
	
	
	protected Model renameByIdentifier(Model model) {
		model = renameByIdentifier(model, model.getProperty(
					CORDIS_TBOX_NS + "identifier"), "id");
		model = renameByIdentifier(model, model.getProperty(
					CORDIS_TBOX_NS + "inraIdentifier"), "in");
		
		return model;
	}
	
	
	protected Model constructForVIVO(Model model) {
		String query = "grant.sparql";
		
		log.debug("Executing query " + query);
        log.debug("Pre-query model size: " + model.size());
        
        construct(SPARQL_RESOURCE_DIR + query, model, NAMESPACE_ETC);
        
        log.debug("Post-query model size: " + model.size());
        
        return model;
	}
	
	@Override
	protected Model mapToVIVO(Model model) {
		
		model = renameByIdentifier(model);
		
		model = constructForVIVO(model);
		
		return rdfUtils.smushResources(model, model.getProperty(
					CORDIS_TBOX_NS + "identifier"));
	}


	@Override
	protected Model filter(Model model) {
		// TODO Auto-generated method stub
		return model;
	}

}
