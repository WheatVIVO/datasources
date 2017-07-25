package org.wheatinitiative.vivo.datasource.connector.openaire;

// Java imports
import java.net.URI;
import java.net.URISyntaxException;
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
import com.hp.hpl.jena.util.ResourceUtils;



public class OpenAire extends ConnectorDataSource implements DataSource {

	public static final Log log = LogFactory.getLog(OpenAire.class);
	
	private static final List<String> METADATA_PREFIXES = Arrays.asList("oaf", "oai_dc");
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
			return new OaiModelIterator(this.getConfiguration().getServiceURI(), METADATA_PREFIXES);
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
		private List<String> metadataPrefixes;
		
		private Model cachedResult = null;
		private Integer totalRecords = null;
		private String resumptionToken = null;
		private boolean projectsDone = false;
		private boolean pubsDone = false;
		
		
		public OaiModelIterator(String repositoryURL, List<String> metadataPrefixes) throws URISyntaxException {
			this.repositoryURI = new URI(repositoryURL);
			this.metadataPrefixes = metadataPrefixes;
		}
		
		
		public boolean hasNext() {
			return (cachedResult != null || !(projectsDone && pubsDone));
		}
		
		
		public Model next() {
			if (cachedResult != null) {
				Model model = cachedResult;
				cachedResult = null;
				return model;
			} else {
				if (projectsDone && pubsDone) {
					throw new RuntimeException("No more items");
				} else {
					// Do multiple fetches depending on the different prefixes.
					// At the moment let's concentrate on projects and
					// publications
					// (datasets might be added later).
					Model projectsModel = ModelFactory.createDefaultModel();
					Model pubsModel = ModelFactory.createDefaultModel();
					Model generalModel = ModelFactory.createDefaultModel();
					
					projectsModel = fetchNextProject(!CACHE);
					
					for (String prefix : metadataPrefixes) {
						pubsModel = fetchNextPublication(!CACHE, prefix);
					}
					
					generalModel.add(projectsModel);
					generalModel.add(pubsModel);
					
					return generalModel;
				}
			}
		}
		
		
		private final static boolean CACHE = true;
		
		private void cacheNext() {
			fetchNextProject(CACHE);
			for (String prefix : metadataPrefixes) {
				fetchNextPublication(CACHE, prefix);
			}
		}
		
		
		private Model fetchNextProject(boolean cacheResult) {
			
			URIBuilder uriB = new URIBuilder(repositoryURI);
			uriB.addParameter("verb", "ListRecords");
			if (resumptionToken != null) {
				uriB.addParameter("resumptionToken", resumptionToken);
			} else {
				uriB.addParameter("set", "projects");
				uriB.addParameter("metadataPrefix", metadataPrefixes.get(0));
				// Only the 1st prefix for this one.
			}
			try {
				String request = uriB.build().toString();
				log.info(request);
				String response = httpUtils.getHttpResponse(request);
				Model model = xmlToRdf.toRDF(response);
				processResumptionToken(model);
				if (resumptionToken == null) {
					projectsDone = true;
					log.info("No more resumption token -- done.");
				}
				if (cacheResult) {
					cachedResult = model;
				}
				return model;
			} catch (Exception e) {
				if (this.resumptionToken != null) {
					this.resumptionToken = guessAtNextResumptionToken(this.resumptionToken);
				}
				throw new RuntimeException(e);
			}
		}
		
		
		private Model fetchNextPublication(boolean cacheResult, String givenPrefix) {
			
			URIBuilder uriB = new URIBuilder(repositoryURI);
			uriB.addParameter("verb", "ListRecords");
			if (resumptionToken != null) {
				uriB.addParameter("resumptionToken", resumptionToken);
			} else {
				uriB.addParameter("set", "openaire");
				// "openaire" -> set for all the pubs
				uriB.addParameter("metadataPrefix", givenPrefix);
			}
			try {
				String request = uriB.build().toString();
				log.info(request);
				String response = httpUtils.getHttpResponse(request);
				Model model = xmlToRdf.toRDF(response);
				processResumptionToken(model);
				if (resumptionToken == null) {
					pubsDone = true;
					log.info("No more resumption token -- done.");
				}
				if (cacheResult) {
					cachedResult = model;
				}
				return model;
			} catch (Exception e) {
				if (this.resumptionToken != null) {
					this.resumptionToken = guessAtNextResumptionToken(this.resumptionToken);
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
		
		
		private void processResumptionToken(Model model) {
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
			this.resumptionToken = token;
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
	
	
	protected Model renameByIdentifier(Model model) {
		model = renameByIdentifier(model, model.getProperty(OPEN_AIRE_TBOX_NS + "identifier"), "id");
		model = renameByIdentifier(model, model.getProperty(OPEN_AIRE_TBOX_NS + "inraIdentifier"), "in");
		model = renameByIdentifier(model, model.getProperty(OPEN_AIRE_TBOX_NS + "idCollection"), "ic");
		return model;
	}
	
	
	private Model renameByIdentifier(Model model, Property identifier, String localNamePrefix) {
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
	
	
	private Model constructForVIVO(Model model) {
		// TODO - Make construct queries to map the data to VIVO.
		return model;
	}
	
	
	@Override
	protected Model mapToVIVO(Model model) {
		model = rdfUtils.renameBNodes(model, NAMESPACE_ETC, model);
		model = renameByIdentifier(model);
		model = constructForVIVO(model);
		return rdfUtils.smushResources(model, model.getProperty(OPEN_AIRE_TBOX_NS + "identifier"));
	}
	
}
