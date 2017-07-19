package org.wheatinitiative.vivo.datasource.connector.openaire;

// Java imports
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

// Apache imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;

// Wheatinitiative imports
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

// Jena inports
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.ResourceUtils;



public class OpenAire extends ConnectorDataSource implements DataSource {

	public static final Log log = LogFactory.getLog(OpenAire.class);
	private static final String OPEN_AIRE_API_URL = "http://api.openaire.eu/";
	private static final String OPEN_AIRE_TBOX_NS = OPEN_AIRE_API_URL;
	private static final String OPEN_AIRE_ABOX_NS = OPEN_AIRE_TBOX_NS + "publications/";
	private static final String NAMESPACE_ETC = OPEN_AIRE_ABOX_NS + "n";
	private static final String SPARQL_RESOURCE_DIR = "/openaire/sparql/";
	private static final int MIN_REST_AFTER_HTTP_REQUEST = 250; // ms
	
	//Use the ingest_Uri created from the XmlToRdf converter, in order to find the total results per query term.
	private static final String INGEST_URI = "http://ingest.mannlib.cornell.edu/generalizedXMLtoRDF/0.1/";
	
	
	@Override
	protected IteratorWithSize<Model> getSourceModelIterator() {
		return new OpenAireModelIterator(this.getConfiguration().getServiceURI());
	}
	
	
	private class OpenAireModelIterator implements IteratorWithSize<Model> {
		
		private String service_URI;
		private List<String> queryTerms;
		private static final int SEARCH_RESULTS_PER_PAGE = 50;
		
		private boolean done = false;
		private HttpUtils httpUtils = new HttpUtils();
		private RdfUtils rdfUtils = new RdfUtils();
		private XmlToRdf xmlToRdf = new XmlToRdf();
		private Map<String, Model> initialResultsCache;
		private Map<String, Integer> totalForQueryTerm;
		private Iterator<String> queryTermIterator;
		private String currentQueryTerm = null;
		private int currentResultForQueryTerm = 1;
		
		public OpenAireModelIterator(String serviceURI) {
			this.service_URI = serviceURI;
			this.queryTerms = getQueryTerms();
			this.queryTermIterator = queryTerms.iterator();
			initialResultsCache = fetchInitialResults();
			totalForQueryTerm = getTotalsForQueryTerms(initialResultsCache);
		}
		
		
		/*
		 * A method to retrieve all the VIVO-project's Ids from a model.
		 */
		public List<String> retrieveProjectsIds() {
			
			List<String> projectIds = new ArrayList<String>();
			
			
			/*
			String selectQueryStr = loadQuery(SPARQL_RESOURCE_DIR + "SELECT-projects-ids.sparql");
			
			ResultSet result = getSparqlEndpoint().getResultSet(selectQueryStr);
			
			QuerySolution querySol;
			RDFNode node;
			
			while (result.hasNext()) {
				querySol = result.next();
				node = qsol.get("project_id");
				
				if (node != null && node.isLiteral()) {
					projectIds.add(node.asLiteral().getLexicalForm());
				}
			}
			*/
			
			projectIds.add("211863");	// Test retrieved data for an example projectID.
			
			return projectIds;
		}
		
		
		/*
		 * Use projectIds as query terms. Later we can use a parameter to set
		 * the wanted querySet.
		 */
		private List<String> getQueryTerms() {
			return retrieveProjectsIds();
		}
		
		
		public boolean hasNext() {
			return ((this.size() > 0) && !done);
		}
		
		
		public Model next() {
			if (currentQueryTerm == null) {
				if (queryTermIterator.hasNext()) {
					currentQueryTerm = queryTermIterator.next();
					currentResultForQueryTerm = 1;
				} else {
					done = true;
					return ModelFactory.createDefaultModel();
				}
			}
			Model searchResultsModel = null;
			if (currentResultForQueryTerm == 1) {
				searchResultsModel = initialResultsCache.get(currentQueryTerm);
			} else {
				searchResultsModel = fetchPublications(currentQueryTerm, currentResultForQueryTerm);
			}
			currentResultForQueryTerm += SEARCH_RESULTS_PER_PAGE;
			if (currentResultForQueryTerm > totalForQueryTerm.get(currentQueryTerm)) {
				if (queryTermIterator.hasNext()) {
					currentQueryTerm = queryTermIterator.next(); // Go to the next query term.
					currentResultForQueryTerm = 1;
				} else {
					done = true;
				}
			}
			return rdfUtils.renameBNodes(searchResultsModel, NAMESPACE_ETC, searchResultsModel);
		}
		
		
		public Integer size() {
			return totalResults(initialResultsCache);
		}
		
		
		private Map<String, Model> fetchInitialResults() {
			Map<String, Model> initialResultsCache = new HashMap<String, Model>();
			for (String queryTerm : queryTerms) {
				initialResultsCache.put(queryTerm, fetchPublications(queryTerm, 1));
			}
			return initialResultsCache;
		}
		
		
		private Map<String, Integer> getTotalsForQueryTerms(Map<String, Model> initialResultsCache) {
			Map<String, Integer> totalsForQueryTerms = new HashMap<String, Integer>();
			for (String queryTerm : initialResultsCache.keySet()) {
				int total = totalResults(initialResultsCache.get(queryTerm));
				totalsForQueryTerms.put(queryTerm, total);
				log.info(total + " results for query term " + queryTerm);
			}
			return totalsForQueryTerms;
		}
		
		
		private Model fetchPublications(String projId, int startingResult) {
			try {
				URIBuilder uriB = new URIBuilder(OPEN_AIRE_API_URL + "search/publications");
				uriB.addParameter("projectID", projId);
				uriB.addParameter("page", Integer.toString((startingResult / SEARCH_RESULTS_PER_PAGE) + 1, 10));
				uriB.addParameter("size", Integer.toString(SEARCH_RESULTS_PER_PAGE, 10));
				uriB.addParameter("format", "xml");
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
		
		
		/*
		 * get total hits for a map of query term to first search result model
		 */
		private int totalResults(Map<String, Model> initialResultsCache) {
			int total = 0;
			for (String queryTerm : initialResultsCache.keySet()) {
				total += totalResults(initialResultsCache.get(queryTerm));
			}
			return total;
		}
		
		
		/*
		 * get total hits for a model representing the first results page of a
		 * search for a given query term
		 */
		private int totalResults(Model model) {
			return getIntValue(INGEST_URI + "total", model);
		}
		
		
		private int getIntValue(String predicateURI, Model model) {
			StmtIterator sit = model.listStatements(null, model.getProperty(predicateURI), (RDFNode) null);
			try {
				while (sit.hasNext()) {
					Statement stmt = sit.next();
					if (stmt.getObject().isLiteral()) {
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
		
	}
	
	
	protected Model renameByIdentifier(Model model, Property identifier, String localNamePrefix) {
		
		Map<Resource, String> idMap = new HashMap<Resource, String>();
		StmtIterator sit = model.listStatements(null, identifier, (RDFNode) null);
		while (sit.hasNext()) {
			Statement stmt = sit.next();
			if (stmt.getObject().isLiteral()) {
				idMap.put(stmt.getSubject(), stmt.getObject().asLiteral().getLexicalForm());
			}
		}
		for (Resource res : idMap.keySet()) {
			ResourceUtils.renameResource(res, OPEN_AIRE_ABOX_NS + localNamePrefix + idMap.get(res));
		}
		
		return model;
	}
	
	
	protected Model renameByIdentifier(Model model) {
		model = renameByIdentifier(model, model.getProperty(OPEN_AIRE_TBOX_NS + "identifier"), "id");
		model = renameByIdentifier(model, model.getProperty(OPEN_AIRE_TBOX_NS + "inraIdentifier"), "in");
		
		return model;
	}
	
	
	protected Model constructForVIVO(Model model) {
		
		List<String> queries = Arrays.asList(
											  "100-publication.sparql"
											 ,"101-publication-article.sparql"
											 ,"110-publication-url.sparql"
											 ,"120-publication-keywords.sparql"
										//	 ,"130-publication-project-connection.sparql"
											 ,"200-authorship.sparql"
											 ,"210-author-vcard-name.sparql"
											 ,"300-journal.sparql"
											 ,"400-publisher-journal.sparql"
											);
		
		for (String query : queries) {
			log.debug("Executing query " + query);
			log.debug("Pre-query model size: " + model.size());
			construct(SPARQL_RESOURCE_DIR + query, model, NAMESPACE_ETC);
			log.debug("Post-query model size: " + model.size());
		}
		
		return model;
	}
	
	
	@Override
	protected Model mapToVIVO(Model model) {
		
		model = renameByIdentifier(model);
		model = constructForVIVO(model);
		
		return rdfUtils.smushResources(model, model.getProperty(OPEN_AIRE_TBOX_NS + "identifier"));
	}
	
	
	@Override
	protected Model filter(Model model) {
		return model;
	}
	
}
