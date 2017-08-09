package org.wheatinitiative.vivo.datasource.connector.openaire;

// Java imports
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

// Jena imports
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;



public class OpenAire extends ConnectorDataSource implements DataSource {
	
	public static final Log log = LogFactory.getLog(OpenAire.class);
	
	private static final String METADATA_PREFIX = "oaf";
	private static final String OPEN_AIRE_TBOX_NS = "http://api.openaire.eu/oai_pmh/";
	private static final String OPEN_AIRE_ABOX_NS = OPEN_AIRE_TBOX_NS + "individual/";
	private static final String NAMESPACE_ETC = OPEN_AIRE_ABOX_NS + "n";
	private static final String SPARQL_RESOURCE_DIR = "/openaire/sparql/";
	
	private static final Property RESUMPTION_TOKEN = ResourceFactory
			.createProperty("http://www.openarchives.org/OAI/2.0//resumptionToken");
	private static final Property TOTAL_RECORDS = ResourceFactory
			.createProperty("http://ingest.mannlib.cornell.edu/generalizedXMLtoRDF/0.1/completeListSize");
	private static final Property VITRO_VALUE = ResourceFactory
			.createProperty("http://vitro.mannlib.cornell.edu/ns/vitro/0.7#value");
	
	private HttpUtils httpUtils = new HttpUtils();
	private XmlToRdf xmlToRdf = new XmlToRdf();
	private RdfUtils rdfUtils = new RdfUtils();
	
	
	@Override
	protected IteratorWithSize<Model> getSourceModelIterator() {
		try {
			return new OaiModelIterator(this.getConfiguration().getServiceURI(), METADATA_PREFIX);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}
	
	
	private class OaiModelIterator implements IteratorWithSize<Model> {
		
		private URI repositoryURI;
		private String metadataPrefix;
		
		private Model cachedResult = null;
		private Integer totalRecords = null;
		private String projectsResumptionToken = null;
		private String pubsResumptionToken = null;
		private boolean projectsDone = false;
		private boolean pubsDone = false;
		
		
		/**
		 * Iterate through OpenAIRE's OAI protocol.
		 */		
		public OaiModelIterator(String repositoryURL, String metadataPrefix) throws URISyntaxException {
			this.repositoryURI = new URI(repositoryURL);
			this.metadataPrefix = metadataPrefix;
		}
		
		
		/**
		 * Check if there are still records to retrieve.
		 */
		public boolean hasNext() {
			return (cachedResult != null || !(projectsDone && pubsDone));
		}
		
		
		public Model next() {
			Model model = ModelFactory.createDefaultModel();
			if (cachedResult != null) {
				model = cachedResult;
				cachedResult = null;
				
			} else {
				// Do multiple fetches depending on the different kind of data.
				
				if (projectsDone) {
					log.info("No more projects.");
				} else {
					model.add( fetchNextProject(!CACHE) );
				}
				
				if (pubsDone) {
					log.info("No more pubs.");
				} else {
					model.add( fetchNextPublication(!CACHE) );
				}
				
				if ( projectsDone && pubsDone ) {
					throw new RuntimeException("No more items!");
				}
			}
			return model;
		}
		
		
		private final static boolean CACHE = true;
		
		private void cacheNext() {
			fetchNextProject(CACHE);
			fetchNextPublication(CACHE);
		}
		
		
		/**
		 * Fetch the projects' related data.
		 */
		private Model fetchNextProject(boolean cacheResult) {
			URIBuilder uriB = new URIBuilder(repositoryURI);
			uriB.addParameter("verb", "ListRecords");
			if (projectsResumptionToken != null) {
				uriB.addParameter("resumptionToken", projectsResumptionToken);
			} else {
				uriB.addParameter("set", "projects");
				uriB.addParameter("metadataPrefix", metadataPrefix);
			}
			try {
				String request = uriB.build().toString();
				log.info(request);
				String response = httpUtils.getHttpResponse(request);
				Model model = xmlToRdf.toRDF(response);
				processResumptionToken(model, "project");
				if (projectsResumptionToken == null) {
					projectsDone = true;
					log.info("No more project's resumption token -- done.");
				}
				if (cacheResult) {
					cachedResult = model;
				}
				return model;
			} catch (Exception e) {
				if (this.projectsResumptionToken != null) {
					this.projectsResumptionToken = guessAtNextResumptionToken(this.projectsResumptionToken);
				}
				throw new RuntimeException(e);
			}
		}
		
		
		/**
		 * Fetch the publications' related data.
		 */
		private Model fetchNextPublication(boolean cacheResult) {
			URIBuilder uriB = new URIBuilder(repositoryURI);
			uriB.addParameter("verb", "ListRecords");
			if (pubsResumptionToken != null) {
				uriB.addParameter("resumptionToken", pubsResumptionToken);
			} else {
				uriB.addParameter("set", "openaire");
				// "openaire" -> resultSet for all the pubs
				uriB.addParameter("metadataPrefix", metadataPrefix);
			}
			try {
				String request = uriB.build().toString();
				log.info(request);
				String response = httpUtils.getHttpResponse(request);
				Model model = xmlToRdf.toRDF(response);
				processResumptionToken(model, "publication");
				if (pubsResumptionToken == null) {
					pubsDone = true;
					log.info("No more pub's resumption token -- done.");
				}
				if (cacheResult) {
					cachedResult = model;
				}
				return model;
			} catch (Exception e) {
				if (this.pubsResumptionToken != null) {
					this.pubsResumptionToken = guessAtNextResumptionToken(this.pubsResumptionToken);
				}
				throw new RuntimeException(e);
			}
		}
		
		
		private String guessAtNextResumptionToken(String resumptionToken) {
			
			try {
				String[] tokens = resumptionToken.split("!");
				int cursor = Integer.parseInt(tokens[1], 10);
				cursor = cursor + 200;
				return tokens[0] + "!" + cursor + "!" + tokens[2] + "!" + tokens[3] + "!" + tokens[4];
			} catch (Exception e) {
				log.error(e, e);
				return null;
			}
		}
		
		
		private void processResumptionToken(Model model, String dataType) {
			
			NodeIterator nit = model.listObjectsOfProperty(RESUMPTION_TOKEN);
			String token = null;
			while (nit.hasNext()) {
				RDFNode n = nit.next();
				if (n.isResource()) {
					if (totalRecords == null) {
						StmtIterator sit = n.asResource().listProperties(TOTAL_RECORDS);
						while (sit.hasNext()) {
							Statement stmt = sit.next();
							if (stmt.getObject().isLiteral()) {
								int total = stmt.getObject().asLiteral().getInt();
								this.totalRecords = new Integer(total);
							}
						}
					}
					StmtIterator sit = n.asResource().listProperties(VITRO_VALUE);
					try {
						while (sit.hasNext()) {
							Statement stmt = sit.next();
							if (stmt.getObject().isLiteral()) {
								token = stmt.getObject().asLiteral().getLexicalForm();
							}
						}
					} finally {
						sit.close();
					}
				}
			}
			log.debug("Token: " + token);
			if ( dataType == "project" )
				this.projectsResumptionToken = token;
			else if ( dataType == "publication" )
				this.pubsResumptionToken = token;
		}
		
		
        /**
         * The size of the model.
         */
		public Integer size() {
			if (totalRecords == null) {
				cacheNext();
			}
			return totalRecords;
		}
		
	}
	
	
	/**
	 * Sparql CONSTRUCT queries to transform the data.
	 */
	private Model constructForVIVO(Model model) {
		
		List<String> queries = Arrays.asList(
											  "100-project.sparql"
											 ,"110-project-title_only.sparql"
											 ,"111-project-title_with_empty_acronym.sparql"
											 ,"112-project-title_with_acronym.sparql"
											 ,"120-project-url.sparql"
											 ,"130-project-date.sparql"
											 ,"140-project-keywords.sparql"
											 ,"150-project-organization.sparql"
											 ,"155-project-organization_address.sparql"
											 ,"160-project-participant_organization.sparql"
											 ,"200-publication.sparql"
											 ,"201-publication_article.sparql"
											 ,"202-publication_manuscript.sparql"
											 ,"203-publication-thesis.sparql"
											 ,"204-publication-conference_paper.sparql"
											 ,"210-publication-url.sparql"
											 ,"220-publication-keywords.sparql"
											 ,"230-publication-authorship.sparql"
											 ,"235-publication-author_vcard_name.sparql"
											 ,"300-journal.sparql"
											 ,"310-journal-publisher.sparql"
											);
		
		for (String query : queries) {
			log.debug("Executing query " + query);
			log.debug("Pre-query model size: " + model.size());
			construct(SPARQL_RESOURCE_DIR + query, model, NAMESPACE_ETC);
			log.debug("Post-query model size: " + model.size());
		}
		
		return model;
	}
	
	
	/**
	 * Transform the raw RDF into VIVO RDF. 
	 */
	@Override
	protected Model mapToVIVO(Model model) {
		
		model = rdfUtils.renameBNodes(model, NAMESPACE_ETC, model);
		model = constructForVIVO(model);
		return model;
	}
	
	
	/**
	 * Get only the query_terms-related general_URIs.
	 */
    protected List<Resource> getRelevantResources(Model model, String type) {
    	
        String queryStr = loadQuery(
                SPARQL_RESOURCE_DIR + "get" + type + "s" + "ForSearchTerm.sparql");
        
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
                    Resource res = soln.getResource(type);
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
    
    
    /**
     * Construct the subgraph of the given project's general_URI.
     * Connect the related organizations' and others' data with each resource's URI.
     */
    private Model constructProjectSubgraph(Resource projectRes, Model model) {
    	// TODO - Construct project's subgraph.
    	
        Model subgraph = ModelFactory.createDefaultModel();
        Map<String, String> substitutions = new HashMap<String, String>();
        substitutions.put("\\?project", "<" + projectRes.getURI() + ">");
        
        subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getProjectSubgraph.sparql", model, 
                NAMESPACE_ETC, substitutions));
        
        // TODO Also add any other project-related data.
        return subgraph;
    }
    
    
    /**
     * Construct the subgraph of the given publication's general_URI.
     * Connect the related organizations' and others' data with each resource's URI.
     */
    private Model constructPublicationSubgraph(Resource publicationRes, Model model) {
    	// TODO - Construct publication's subgraph.
    	
        Model subgraph = ModelFactory.createDefaultModel();
        Map<String, String> substitutions = new HashMap<String, String>();
        substitutions.put("\\?publication", "<" + publicationRes.getURI() + ">");
        
        subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getPublicationSubgraph.sparql", model, 
                NAMESPACE_ETC, substitutions));
        
        // Also add any other publication-related data.
    	return subgraph;
    }
    
    
	/**
	 * Filter the model so that we keep only data related to the query terms.
	 */
    @Override
    protected Model filter(Model model) {
    	
        Model filtered = ModelFactory.createDefaultModel();
        List<Resource> relevantResources = null;
        
        relevantResources = getRelevantResources(model, "Project");
        log.info(relevantResources.size() + " project-relevant resources");
        for (Resource res : relevantResources) {
            filtered.add(constructProjectSubgraph(res, model));
        }
        
        relevantResources = getRelevantResources(model, "Publication");
        log.info(relevantResources.size() + " publication-relevant resources");
        for (Resource res : relevantResources) {
            filtered.add(constructPublicationSubgraph(res, model));
        }
        
        return filtered;
    }
    
}
