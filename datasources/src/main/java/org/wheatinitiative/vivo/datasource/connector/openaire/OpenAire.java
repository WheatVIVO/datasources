package org.wheatinitiative.vivo.datasource.connector.openaire;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.connector.orcid.Name;
import org.wheatinitiative.vivo.datasource.connector.orcid.NameProcessor;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;

public class OpenAire extends ConnectorDataSource implements DataSource {
	
	public static final Log log = LogFactory.getLog(OpenAire.class);
	
	private static final String OPEN_AIRE_API_URL = "http://api.openaire.eu/";
	private static final String SEARCH_PUBLICATIONS = OPEN_AIRE_API_URL + "search/publications";
	private static final String SEARCH_PROJECTS = OPEN_AIRE_API_URL + "search/projects";
	private static final String OPEN_AIRE_ABOX_NS = "https://wheatvivo.org/individual/openaire-";
	
	private static final String NAMESPACE_ETC = OPEN_AIRE_ABOX_NS + "n";
	
	private static final int RESULTS_PER_PAGE = 50;
	
	private static final String SPARQL_RESOURCE_DIR = "/openaire/sparql/";
	
	private HttpUtils httpUtils = new HttpUtils();
	private XmlToRdf xmlToRdf = new XmlToRdf();
	private RdfUtils rdfUtils = new RdfUtils();
	
	
	@Override
	protected IteratorWithSize<Model> getSourceModelIterator() {
	    try {
	        return new OpenAireIterator();
	    } catch (IOException e) {
            log.error(e, e);
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            log.error(e, e);
            throw new RuntimeException(e);
        }
	}
	
	// narrow results to stay close to 10000 result search limit
	private String adjustQueryTerm(String queryTerm) {
	    return queryTerm + " crop";
	}
	
	private class OpenAireIterator implements IteratorWithSize<Model> {		

	    private List<Integer> totals = new ArrayList<Integer>();
		private int currentQueryTerm = 0;
		private int currentPage = 1;
		private String fromDateStr;				
		
		public OpenAireIterator() throws IOException, URISyntaxException {
		    // search last two years to stay under retrieval limit
            // TODO support multiple year ranges to retrieve full history
            Date date = Calendar.getInstance().getTime();
            date = DateUtils.addYears(date, -2);
            SimpleDateFormat ymdFormat = new SimpleDateFormat("yyyy-MM-dd");
            this.fromDateStr= ymdFormat.format(date);
		    int queryTermsSize = getConfiguration().getQueryTerms().size();
		    log.info(queryTermsSize + " query terms");
		    Pattern r = Pattern.compile("<total>(\\d+)</total>");
			for(String keyword : getConfiguration().getQueryTerms()) {
			    URIBuilder b = new URIBuilder(SEARCH_PUBLICATIONS);
			    b.addParameter("size", Integer.toString(RESULTS_PER_PAGE));
			    b.addParameter("keywords", adjustQueryTerm(keyword));			    
			    b.addParameter("fromDateAccepted", fromDateStr);
			    b.addParameter("format", "xml");
			    String url = b.toString();
			    log.info("Retrieving " + url);
			    String xml = httpUtils.getHttpResponse(url);
			    //log.info(xml);
			    Matcher m = r.matcher(xml);
			    if(m.find()) {
			        String recordsStr = m.group(1);
			        log.info("Found " + recordsStr + " records for " + keyword);
			        int records = Integer.parseInt(recordsStr); 
			        int total = records / RESULTS_PER_PAGE;
			        if(records % RESULTS_PER_PAGE > 0) {
			            total++;
			        }
			        totals.add(total);
			    }
			}
		}		
		
		/**
		 * Check if there are still records to retrieve.
		 */
		public boolean hasNext() {
			return (currentQueryTerm < getConfiguration().getQueryTerms().size() 
			        && currentPage <= totals.get(currentQueryTerm));
		}		
		
		/**
		 * Iterate over different chunks of data.
		 */
		public Model next() {
		    try {
		        Model nextModel = fetchNextPublication();
		        return nextModel;
		    } finally {
		        currentPage++;
		        if(currentPage > totals.get(currentQueryTerm)) {
		            currentQueryTerm++;
		            currentPage = 1;
		        }
		    }
		}
		
		/**
		 * Fetch the publications' related data.
		 */
		private Model fetchNextPublication() {		
            try {
			    URIBuilder uriB = new URIBuilder(SEARCH_PUBLICATIONS);
			    uriB.addParameter("size", Integer.toString(RESULTS_PER_PAGE));
			    uriB.addParameter("page", Integer.toString(currentPage));
			    uriB.addParameter("keywords", adjustQueryTerm(getConfiguration().getQueryTerms()
			            .get(currentQueryTerm)));
			    uriB.addParameter("fromDateAccepted", fromDateStr);
			    uriB.addParameter("format", "xml");
				String request = uriB.build().toString();
				log.info(request);
				String response = httpUtils.getHttpResponse(request);
				Model model = xmlToRdf.toRDF(response);
				try {
				    model.add( getPubRelatedProjects( model ) );
				} catch (Exception e) {
				    log.error(e, e);
				}
		        model = rdfUtils.renameBNodes(model, NAMESPACE_ETC + "-" + currentPage + "-n", model);
				model = splitNames(model);
				return model;
			} catch (Exception e) {
			    log.error(e, e);
				throw new RuntimeException(e);
			}
		}
		
		
	    /**
	     * Retrieve the pub-related projects for the current iteration.
	     */
	    Model getPubRelatedProjects( Model currentModel ) {
	    	
	    	Model relatedProjectsModel = ModelFactory.createDefaultModel();
	    	
	    	List<String> tempProjectIds = retrieveProjectsIds( currentModel );
	    	ListIterator<String> listIt = tempProjectIds.listIterator();
	    	
	    	while ( listIt.hasNext() ) {
				try {
					URIBuilder uriB = new URIBuilder(SEARCH_PROJECTS);
					uriB.addParameter( "keywords", "\"" + listIt.next().toString() + "\"" );
					uriB.addParameter( "format", "xml" );
					
					String request = uriB.build().toString();
					log.info(request);
					String response = httpUtils.getHttpResponse(request);
					relatedProjectsModel = xmlToRdf.toRDF(response);
					
				} catch (Exception e) {
					log.error(e, e);
					throw new RuntimeException(e);
				}
	    	}
	    	
	    	return relatedProjectsModel;
	    }
		
		
		/**
		 * A method to retrieve all the projects' IDs from a model.
		 */
	    public List<String> retrieveProjectsIds( Model currentModel ) {
	    	
			String selectQueryStr = loadQuery(SPARQL_RESOURCE_DIR + "SELECT-project-id.sparql");
			QueryExecution selectExec = QueryExecutionFactory.create(selectQueryStr, currentModel);
			
			List<String> tempProjectIds = new ArrayList<String>();
			try {
				ResultSet result = selectExec.execSelect();
				QuerySolution qsol;
				RDFNode node;
				
				while ( result.hasNext() ) {
					qsol = result.next();
					node = qsol.get("projectId");
					
					if ( node != null && node.isLiteral()) {
						String value = node.asLiteral().getLexicalForm();
						tempProjectIds.add(value);
					}
				}
				return tempProjectIds;
				
			} catch (Exception e) {
	    		log.error(e, e);
				throw new RuntimeException(e);
	    	} finally {
	    		if ( selectExec != null )
	    			selectExec.close();
	    	}
	    }    
	    
	    /**
		 * Fetch the projects' related data.
		 */
		private Model fetchNextProject(boolean cacheResult) {
	        try {
			    URIBuilder uriB = new URIBuilder(OPEN_AIRE_API_URL);		
				String request = uriB.build().toString();
				log.info(request);
				String response = httpUtils.getHttpResponse(request);
				Model model = xmlToRdf.toRDF(response);
				return model;				
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}		
		
        /**
         * The size of the model.
         */
		public Integer size() {
			int size = 0;
			for(int total : totals) {
			    size += total;
			}
			return size;
		}
		
	}
	
	private Model splitNames(Model rdf) {
        NameProcessor nameProc = new NameProcessor();
        List<Statement> additions = new ArrayList<Statement>();
        StmtIterator creatorIt = rdf.listStatements(null, rdf.getProperty(
                XmlToRdf.GENERIC_NS + "creator"), (RDFNode) null);
        while(creatorIt.hasNext()) {            
            Statement creatorStmt = creatorIt.next();
            if(!creatorStmt.getObject().isURIResource()) {
                continue;
            }
            Resource creator = creatorStmt.getObject().asResource();
            if(rdf.contains(creator, rdf.getProperty(
                    XmlToRdf.GENERIC_NS + "surname"), (RDFNode) null)) {
                continue;
            }
            StmtIterator nameIt = rdf.listStatements(creator, rdf.getProperty(
                            XmlToRdf.VITRO_NS + "value"), (RDFNode) null);
            while(nameIt.hasNext()) {
                Statement nameStmt = nameIt.next();
                if(nameStmt.getObject().isLiteral()) {
                    Name name = nameProc.parseName(nameStmt.getObject().asLiteral().getLexicalForm());
                    additions.add(ResourceFactory.createStatement(
                            nameStmt.getSubject(), rdf.getProperty(
                                    XmlToRdf.GENERIC_NS + "name"), 
                            ResourceFactory.createPlainLiteral(name.getGivenName())));
                    additions.add(ResourceFactory.createStatement(
                            nameStmt.getSubject(), rdf.getProperty(
                                    XmlToRdf.GENERIC_NS + "surname"), 
                            ResourceFactory.createPlainLiteral(name.getFamilyName())));
                }
            }
        }
        rdf.add(additions);
        return rdf;
    }
	
	
	/**
	 * Transform the raw RDF into VIVO RDF. 
	 */
	@Override
	protected Model mapToVIVO(Model model) {
		
		model = constructForVIVO(model);
		
		return model;
	}
	
	
	/**
	 * Sparql CONSTRUCT queries to transform the data.
	 */
	private Model constructForVIVO(Model model) {
	    construct(SPARQL_RESOURCE_DIR + "100-project.sparql", model, NAMESPACE_ETC);
        model = renameByIdentifier(model, model.getProperty(
                XmlToRdf.GENERIC_NS + "projectId"), OPEN_AIRE_ABOX_NS, "project-");
        model = renameByIdentifier(model, model.getProperty(
                XmlToRdf.GENERIC_NS + "grantId"), OPEN_AIRE_ABOX_NS, "grant-");
		List<String> queries = Arrays.asList(											  
											 "110-project-title_only.sparql"
											 ,"111-project-title_with_empty_acronym.sparql"
											 ,"112-project-title_with_acronym.sparql"
											 //,"120-project-vcard_url.sparql"
											 ,"130-project-date.sparql"
											 ,"140-project-keywords.sparql"
											 //,"150-project-organization.sparql"
											 //,"155-project-organization_vcard_address.sparql"
											 //,"160-project-participant_organization.sparql"
											 ,"200-publication.sparql");
		for (String query : queries) {
            log.debug("Executing query " + query);
            log.debug("Pre-query model size: " + model.size());
            long preSize = model.size();
            construct(SPARQL_RESOURCE_DIR + query, model, NAMESPACE_ETC);
            log.debug("Post-query model size: " + model.size());
            if(model.size() == preSize) {
                log.info(query + " produced no results");
            }
        }
		model = renameByIdentifier(model, model.getProperty(
                XmlToRdf.GENERIC_NS + "pubId"), OPEN_AIRE_ABOX_NS, "doi-");
		queries = Arrays.asList(
											 "201-publication_article.sparql"
											 ,"202-publication_manuscript.sparql"
											 ,"203-publication-thesis.sparql"
											 ,"204-publication-conference_paper.sparql"
											 //,"210-publication-vcard_url.sparql"
											 ,"215-publication-year.sparql"
											 ,"220-publication-keywords.sparql"
											 ,"230-publication-authorship.sparql"
											 //,"235-publication-author_vcard_name.sparql"
											 ,"300-project-publication-connection.sparql"
											 ,"400-journal.sparql"
											 ,"410-journal-publisher.sparql"
											);
		
		for (String query : queries) {
		    log.debug("Executing query " + query);
            log.debug("Pre-query model size: " + model.size());
            long preSize = model.size();
            construct(SPARQL_RESOURCE_DIR + query, model, NAMESPACE_ETC);
            log.debug("Post-query model size: " + model.size());
            if(model.size() == preSize) {
                log.info(query + " produced no results");
            }
		}
		
		return model;
	}
	
	/**
	 * Filter the model so that we keep only data related to the query terms.
	 */
    @Override
    protected Model filter(Model model) {
        if(false) {
            return model;
        }
        model = this.filterGeneric(model);
        Model filtered = ModelFactory.createDefaultModel();
        List<Resource> relevantResources = null;
        
        relevantResources = getRelevantResources(model, "Publication");
        log.info(relevantResources.size() + " publication-relevant resources");
        for (Resource res : relevantResources) {
            filtered.add(constructPublicationSubgraph(res, model));
        }
        
        relevantResources = getRelevantResources(model, "Project");
        log.info(relevantResources.size() + " project-relevant resources");
        for (Resource res : relevantResources) {
            filtered.add(constructProjectSubgraph(res, model));
        }
        
        return filtered;
    }
	
    
	/**
	 * Get only the query_terms-related entities' URIs.
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
                    
                    //  If we are retrieving projects, then we should also retrieve the related grants.
                    if( type == "Project" ) {
                        res = soln.getResource("Grant");
                        if(res != null) {
                            relevantResources.add(res);
                        }
                    }
                }
                log.info(count + " relevant resources for query term " + queryTerm);
                
            } catch (Exception e) {
            	log.error(e, e);
            	throw new RuntimeException(e);
            } finally {
                if(qe != null) {
                    qe.close();
                }
            }
        }
        
        return relevantResources;
    }
    
    
    /**
     * Construct the subgraph of the given publication's URI.
     * Connect the related organizations' and others' data with each resource's URI.
     */
    private Model constructPublicationSubgraph(Resource publicationRes, Model model) {
    	
        Model subgraph = ModelFactory.createDefaultModel();
        Map<String, String> substitutions = new HashMap<String, String>();
        substitutions.put("\\?entity", "<" + publicationRes.getURI() + ">");
        
        subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getEntitySubgraph.sparql", model, 
                NAMESPACE_ETC, substitutions));
        
		subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getRelatedVcards.sparql", model, 
                NAMESPACE_ETC, substitutions));
                
		subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getRelatedAuthorships.sparql", model, 
                NAMESPACE_ETC, substitutions));
                
		subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getRelatedJournals.sparql", model, 
                NAMESPACE_ETC, substitutions));
		
		subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getRelatedDateTimeValues.sparql", model, 
                NAMESPACE_ETC, substitutions));
        
    	return subgraph;
    }
    
    
    /**
     * Construct the subgraph of the given project's URI.
     * Connect the related organizations' and others' data with each resource's URI.
     */
    private Model constructProjectSubgraph(Resource projectRes, Model model) {
    	
        Model subgraph = ModelFactory.createDefaultModel();
        Map<String, String> substitutions = new HashMap<String, String>();
        substitutions.put("\\?entity", "<" + projectRes.getURI() + ">");
        
        subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getEntitySubgraph.sparql", model, 
                NAMESPACE_ETC, substitutions));
        
        subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getRelatedDateTimeGraphs.sparql", model, 
                NAMESPACE_ETC, substitutions));
        
		subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getRelatedVcards.sparql", model, 
                NAMESPACE_ETC, substitutions));
        
		subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getRelatedOrganizations.sparql", model, 
                NAMESPACE_ETC, substitutions));
        
        return subgraph;
    }

    @Override
    protected String getPrefixName() {
        return "openaireeeee";
    }
    
}
