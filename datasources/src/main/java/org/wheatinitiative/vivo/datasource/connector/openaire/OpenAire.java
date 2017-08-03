package org.wheatinitiative.vivo.datasource.connector.openaire;

// Java imports
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

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
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;



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
	
	
	/*
	 * We still have to filter the results, because we can't use a query term at
	 * the protocol level.
	 */
	
	
	private class OaiModelIterator implements IteratorWithSize<Model> {
		
		private URI repositoryURI;
		private String metadataPrefix;
		
		private Model cachedResult = null;
		private Integer totalRecords = null;
		private String projectsResumptionToken = null;
		private String pubsResumptionToken = null;
		private boolean projectsDone = false;
		private boolean pubsDone = false;
		
		
		public OaiModelIterator(String repositoryURL, String metadataPrefix) throws URISyntaxException {
			this.repositoryURI = new URI(repositoryURL);
			this.metadataPrefix = metadataPrefix;
		}
		
		
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
					log.info("No more projects resumption token -- done.");
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
				processResumptionToken(model, "pub");
				if (pubsResumptionToken == null) {
					pubsDone = true;
					log.info("No more pubs resumption token -- done.");
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
			else if ( dataType == "pub" )
				this.pubsResumptionToken = token;
		}
		
		
		public Integer size() {
			if (totalRecords == null) {
				cacheNext();
			}
			return totalRecords;
		}

	}
	
	
    @Override
    protected Model filter(Model model) {
		// TODO - Filter the retrieved results so that we work only with the
		// wheat-related data.
        return model;
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
	 * Transform raw RDF into VIVO RDF. 
	 */
	@Override
	protected Model mapToVIVO(Model model) {
		
		model = rdfUtils.renameBNodes(model, NAMESPACE_ETC, model);
		model = constructForVIVO(model);
		return rdfUtils.smushResources(model, model.getProperty(OPEN_AIRE_TBOX_NS + "identifier"));
	}
	
}
