package org.wheatinitiative.vivo.datasource.connector.cordis;

// Java imports
import java.util.ArrayList;
import java.util.Arrays;
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

//WheatInitiative imports
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
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;



public class Cordis extends ConnectorDataSource implements DataSource {
	
    public static final Log log = LogFactory.getLog(Cordis.class);
    
    private static final String CORDIS_TBOX_NS = "http://cordis.europa.eu/";
    private static final String CORDIS_ABOX_NS = CORDIS_TBOX_NS + "individual/";
    private static final String NAMESPACE_ETC = CORDIS_ABOX_NS + "n";
    private static final String SPARQL_RESOURCE_DIR = "/cordis/sparql/";
    private static final int MIN_REST_AFTER_HTTP_REQUEST = 250; //ms
    
    
    /**
     * Iterate through this data source.
     */
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
        private static final int SEARCH_RESULTS_PER_PAGE = 30;
        // Cordis' website can't handle more than 30 results per page..
        
        private boolean done = false;
        private HttpUtils httpUtils = new HttpUtils();
        private XmlToRdf xmlToRdf = new XmlToRdf();
        private RdfUtils rdfUtils = new RdfUtils();
        private Map<String, Model> initialResultsCache;
        private Map<String, Integer> totalForQueryTerm;
        private Iterator<String> queryTermIterator;
        private String currentQueryTerm;
        private int currentResultForQueryTerm = 1;
	    
        
        /**
         * Iterate through Cordis.
         */
        public CordisModelIterator(String serviceURI, List<String> queryTerms) {
            this.service_URI = serviceURI;
            this.queryTerms = queryTerms;
            this.queryTermIterator = queryTerms.iterator();
            initialResultsCache = fetchInitialResults();
            totalForQueryTerm = getTotalsForQueryTerms(initialResultsCache);
        }
		
        
        /**
         * Check if there are still records to retrieve.
         */
        public boolean hasNext() {
            return ((this.size() > 0) && !done);
        }
        
        
        /**
         * Iterate through Cordis pages for each query term and retrieve the records.
         */
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
        
        
        /**
         * Retrieve the first page of results for every query term,
         * in order to know how many results to expect from each query term.
         */
        private Map<String, Model> fetchInitialResults() {
        	
            Map<String, Model> initialResultsCache = new HashMap<String, Model>();
            for (String queryTerm : queryTerms) {
                initialResultsCache.put(queryTerm, fetchResults(queryTerm, 1));
            }
            return initialResultsCache;
        }
        
        
        /**
         * Retrieve a page full of the query_term-related records.
         */
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
        
        
        /**
         * Get the xml data of every record.
         */
        private Model getResourcesForSearchResults(Model searchResultsModel) {
        	
            Model resourcesModel = ModelFactory.createDefaultModel();
            Collection<String> resultURIs = getResultURIs(searchResultsModel);
            for(String resultURI : resultURIs) {
                try {
                    log.info("Requesting result " + resultURI);
                    String response = httpUtils.getHttpResponse(resultURI);
                    try {
                    	resourcesModel.add(xmlToRdf.toRDF(response));
                    } catch (Exception e) {
                    	// More error checks to be added later. HttpUtil has to give us access.
                    	log.info("After requesting: " + resultURI + "/n"
                    						+ "we propably encountered an HTTP:404"
                    						+ "- \"Not found!\" error."
                    						+ "/nCheck the debug log for more.");
                    	log.debug(e);
                    	continue;
                    }
                    Thread.sleep(MIN_REST_AFTER_HTTP_REQUEST);                                        
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return rdfUtils.renameBNodes(
                    resourcesModel, NAMESPACE_ETC, resourcesModel);
        }
        
        
        /**
         * Construct a set of the Cordis records' URLs.
         * We use two methods in order to be easier for other type of data to be added in the future.
         */
        private Collection<String> getResultURIs(Model searchResultsModel) {
            /*
             * It seems that we are not getting scientific publications from Cordis.
             * We only get abstracts describing what the real publications include,
             * but no link to the real publications.
             * Also the different types of results that we are getting,
             * are mostly reports to the EU commission which are not scientific publications.
             */
        	
            List<String> resultURIs = new ArrayList<String>();
            resultURIs.addAll(getResultURIs(searchResultsModel, "project"));
            return resultURIs;
        }
        
        
        /**
         * Construct a set of the Cordis records' URLs.
         * That way, we retrieve every record independently.
         */
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
        
        
        /**
         * Construct the individual record's URL in order to retrieve it from Cordis.
         */
        private String makeResultURI(String rcnValue, String type) {
            return CORDIS_TBOX_NS + type + "/rcn/" + rcnValue + "_en.xml";  
        }
        
        
        /**
         * Get the number of the total results that can be retrieved by using all of the query terms.
         */
        private Map<String, Integer> getTotalsForQueryTerms(Map<String, Model> initialResultsCache) {
        	/*
        	 * Note that the actual number of records used is smaller,
        	 * since we only keep the projects from Cordis.
        	 */
        	Map<String, Integer> totalsForQueryTerms = new HashMap<String, Integer>();
            for(String queryTerm : initialResultsCache.keySet()) {
                int total = totalResults(initialResultsCache.get(queryTerm));
                totalsForQueryTerms.put(queryTerm, total);
                log.info(total + " results for query term " + queryTerm);
            }
            return totalsForQueryTerms;
        }
        
        
        /**
         * Get total hits for a map of query term to first search result model
         */
        private int totalResults(Map<String, Model> initialResultsCache) {
            int total = 0;
            for(String queryTerm : initialResultsCache.keySet()) {
                total += totalResults(initialResultsCache.get(queryTerm));
            }
            return total;
        }
        
        
        /**
         * Get total hits for a model representing the first results page 
         * of a search for a given query term 
         */
        private int totalResults(Model model) {
            return getIntValue(CORDIS_TBOX_NS + "totalHits", model);
        }
        
        
        /**
         * Get the integer value from the model.
         */
        private int getIntValue(String predicateURI, Model model) {
            StmtIterator sit = model.listStatements(
                    null, model.getProperty(predicateURI), (RDFNode) null);
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
        
        
        /**
         * The size of the model.
         */
        public Integer size() {
            return totalResults(initialResultsCache);
        }
        
    }
	
	
	/**
	 * Transform raw RDF into VIVO RDF.
	 */
	@Override
	protected Model mapToVIVO(Model model) {
		
		model = constructForVIVO(model);
		
		return model;
	}
	
	
	/**
	 * Makes use of Sparql's CONSTRUCT queries to construct VIVO-RDF data.
	 */
	protected Model constructForVIVO(Model model) {
		List<String> queries =  Arrays.asList(
								"100-project.sparql",
								"110-project-date.sparql",
								"120-project-vcard_url.sparql",
								"200-organization.sparql",
								"210-organization-project-adminrole.sparql",
								"211-orgaization-project-participantrole.sparql",
								"212-organization-project-generalrole.sparql",
								"220-organization-department.sparql",
								"230-organization-vcard_address.sparql"
								);
		
        for(String query : queries) {
        	log.debug("Executing query " + query);
            log.debug("Pre-query model size: " + model.size());
            construct(SPARQL_RESOURCE_DIR + query, model, NAMESPACE_ETC);
            log.debug("Post-query model size: " + model.size());
        }
        return model;
	}
	
	
	/**
	 * Filter the model so that we keep only the query_terms-related data.
	 */
	@Override
	protected Model filter(Model model) {
		// No filtering is needed, since we retrieve already-filtered data.
		return model;
	}
	
}
