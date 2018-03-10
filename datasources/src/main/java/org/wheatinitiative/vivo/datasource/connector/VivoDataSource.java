package org.wheatinitiative.vivo.datasource.connector;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.RiotNotFoundException;
import org.wheatinitiative.vitro.webapp.ontology.update.KnowledgeBaseUpdater;
import org.wheatinitiative.vitro.webapp.ontology.update.UpdateSettings;
import org.wheatinitiative.vivo.datasource.VivoVocabulary;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.classpath.ClasspathUtils;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;

public class VivoDataSource extends ConnectorDataSource {

    private static final String SEARCH_CONTROLLER = "search";
    private static final String QUERYTEXT_PARAM = "querytext";
    private static final String XML_PARAM = "xml";
    private static final String XML_VALUE = "1";
    private static final String HITS_PER_PAGE_PARAM = "hitsPerPage";
    private static final String HITS_PER_PAGE = "100";
    private static final String CLASSGROUP_PARAM = "classgroup";
    private static final String MESH = "http://id.nlm.nih.gov/mesh/";
    
    private Log log = LogFactory.getLog(VivoDataSource.class);
    protected Model result;
    protected HttpUtils httpUtils = new HttpUtils();
    protected XmlToRdf xmlToRdf = new XmlToRdf();
    
    private final static int MIN_REST = 125; // ms between linked data requests
    private static final String RESOURCE_PATH = "/vivo/update15to16/"; 
    private static final String SPARQL_PATH = "/vivo/sparql/";
    private static final int NUMBER_OF_FILTERS = 27;
    // the maximum number of times the filter rules will be applied to a given
    // model, even if they are still finding triples to remove
    private static final int MAX_FILTER_ITERATIONS = 8;
    
    protected String getRemoteVivoURL() {
        return this.getConfiguration().getServiceURI();
    }
    
    protected Model mapToVIVO(Model model) {
        // no work required in standard (VIVO 1.6+) VIVO data source
        return model;
    }
    
    protected static int prefixIteration = 0;
    
    protected Model updateToOneSix(Model model) {
        UpdateSettings settings = new UpdateSettings();
        settings.setABoxModel(model);
        prefixIteration++;
        String localNamePrefix = "vivo-n" + prefixIteration + "-"; 
        settings.setDefaultNamespace(
                VivoVocabulary.DEFAULT_NAMESPACE + localNamePrefix);
        settings.setDiffFile(resolveResource("diff.tab.txt"));
        settings.setSparqlConstructAdditionsDir(resolveResource(
                "sparqlConstructs/additions"));
        settings.setSparqlConstructDeletionsDir(resolveResource(
                "sparqlConstructs/deletions"));
        OntModel oldTBoxModel = ModelFactory.createOntologyModel(
                OntModelSpec.OWL_MEM);
        String oldTBoxDir = resolveResource("oldVersion");
        ClasspathUtils cpu = new ClasspathUtils();
        for(String tboxFile : cpu.listFilesInDirectory(oldTBoxDir)) {
            InputStream is = this.getClass().getResourceAsStream(tboxFile);
            oldTBoxModel.read(is, null, "RDF/XML");
        }
        settings.setOldTBoxModel(oldTBoxModel);
        settings.setNewTBoxModel(oldTBoxModel); // doesn't matter
        KnowledgeBaseUpdater kbu = new KnowledgeBaseUpdater(settings);
        try {
            kbu.update();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return model;
    }
    
    private String resolveResource(String relativePath) {
        return RESOURCE_PATH + relativePath;
    }
    
    protected List<String> getUrisFromSearchResults(String vivoUrl, 
            String querytext) throws URISyntaxException, 
            IOException {
        return getUrisFromSearchResults(vivoUrl, querytext, null);
    }
    
    protected List<String> getUrisFromSearchResults(String vivoUrl, 
            String querytext, String classgroupURI) throws URISyntaxException, 
                IOException {
        List<String> resultUris = new ArrayList<String>();
        URIBuilder builder = new URIBuilder(vivoUrl);
        if(builder.getPath() != null && builder.getPath().endsWith("/")) {
            builder.setPath(builder.getPath() + SEARCH_CONTROLLER);    
        } else if (builder.getPath() != null){
            builder.setPath(builder.getPath() + "/" + SEARCH_CONTROLLER);
        } else {
            builder.setPath(SEARCH_CONTROLLER);
        }        
        builder.addParameter(QUERYTEXT_PARAM, querytext);
        builder.addParameter(XML_PARAM, XML_VALUE);
        builder.addParameter(HITS_PER_PAGE_PARAM, HITS_PER_PAGE);
        if(classgroupURI != null) {
            builder.addParameter(CLASSGROUP_PARAM, classgroupURI);
        }
        URI nextPageUrl = builder.build();
        while(nextPageUrl != null) {
            log.info("Requesting " + nextPageUrl.toString());
            String searchResult = httpUtils.getHttpResponse(
                    nextPageUrl.toString());
            String nextPage = null;
            try {
                Model resultsModel = xmlToRdf.toRDF(searchResult);
                resultUris.addAll(getResultUris(resultsModel));
                nextPage = getNextPageUrl(resultsModel);
            } catch (RuntimeException e) {
                if(e.getCause() != null && e.getCause().getMessage() != null 
                        && e.getCause().getMessage().contains(
                                "Premature end of file")) {
                    // (Old) VIVOs seem to respond with empty search results XML
                    // documents when there are no   
                    // Logging exception only to allow other searches to continue.
                    log.info("Empty results document returned by VIVO " +
                            "for query text " + querytext + "\nRequest URL " 
                            + nextPageUrl.toString());
                } else {                    
                    throw e;
                }
            }
            if(nextPage == null) {
                nextPageUrl = null;
            } else {
                // VIVO seems to have a bug where the classgroup filter 
                // isn't retained in the next page URL, so we
                // have to put it back in (yay!)
                if(!nextPage.startsWith("/")) {
                    nextPage = "/" + nextPage;
                }
                URIBuilder partsGetter = new URIBuilder("http://example.com" + nextPage);
                URIBuilder nextPageBuilder = new URIBuilder(vivoUrl);
                nextPageBuilder.setPath(partsGetter.getPath());
                nextPageBuilder.setParameters(partsGetter.getQueryParams());
                if(classgroupURI != null) {
                    nextPageBuilder.addParameter(CLASSGROUP_PARAM, 
                            classgroupURI);
                }
                nextPageUrl = nextPageBuilder.build();
            }
        }
        return resultUris;
    }
    
    private List<String> getResultUris(Model model) {
        return getValuesByName("uri", model);
    }
    
    private String getNextPageUrl(Model model) {
        List<String> nextPageValues = getValuesByName("nextPage", model);
        if(nextPageValues.isEmpty()) {
            return null;
        } else {
            return nextPageValues.get(0);
        }
    }
    
    private List<String> getValuesByName(String nameStr, Model model) {
        List<String> values = new ArrayList<String>();
        Property name = model.getProperty(XmlToRdf.GENERIC_NS + "name");
        Property value = model.getProperty(XmlToRdf.VITRO_NS + "value");
        ResIterator resIt = model.listSubjectsWithProperty(name, nameStr);
        while(resIt.hasNext()) { 
            Resource boxForValue = resIt.next();
            StmtIterator sit = model.listStatements(
                    boxForValue, value, (RDFNode) null);
            while(sit.hasNext()) {
                Statement stmt = sit.next();
                try {
                    values.add(stmt.getObject().asLiteral().getLexicalForm());
                } catch (Exception e) {
                    log.error("Invalid " + nameStr + " value " 
                            + stmt.getObject());
                }
            }
        }
        return values;
    }
    
    @Override
    protected Model filter(Model model) {
        model = positionsToTopLevelOrgs(model);
        log.debug(model.size() + " before filtering");
        int iterations = 0;
        long difference = -1;;
        while(difference != 0 && iterations < MAX_FILTER_ITERATIONS) {
            Model removalModel = ModelFactory.createDefaultModel();
            iterations++;
            long before = model.size();
            for(int i = 1; i <= NUMBER_OF_FILTERS; i++) {
                long start = System.currentTimeMillis();
                removalModel.add(constructQuery(
                        SPARQL_PATH + "filter" + i + ".rq", model, "", null));
                log.debug((System.currentTimeMillis() - start) + " to run filter" + i);
            }
            model.remove(removalModel);
            log.debug("Removed " + removalModel.size() + " triples on iteration " + iterations);
            difference = model.size() - before;
        }
        model = reassignType(VivoVocabulary.FUNDING_ORG, VivoVocabulary.ORGANIZATION, model);
        model = reassignType(VivoVocabulary.ACADEMIC_ARTICLE, VivoVocabulary.ARTICLE, model);
        model = reassignType(VivoVocabulary.JOURNAL_ARTICLE, VivoVocabulary.ARTICLE, model);
        log.debug(model.size() + " after filtering");        
        return model;
    }
    
    private Model reassignType(Resource fromType, Resource toType, Model model) {
        Model additions = ModelFactory.createDefaultModel();
        Model retractions = ModelFactory.createDefaultModel();
        StmtIterator sit = model.listStatements(null, RDF.type, fromType);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            retractions.add(stmt);
            additions.add(stmt.getSubject(), RDF.type, toType);
        }
        sit = model.listStatements(null, VivoVocabulary.MOST_SPECIFIC_TYPE, fromType);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            retractions.add(stmt);
            additions.add(stmt.getSubject(), VivoVocabulary.MOST_SPECIFIC_TYPE, toType);
        }
        model.remove(retractions);
        model.add(additions);
        return model;
    }
    
    /**
     * Connect positions only to top-level organizations, recording
     * the immediate department in wi:immediateOrgName data property
     * @return the supplied model with appropriate statements modified
     * relating to positions
     */
    protected Model positionsToTopLevelOrgs(Model model) {
        Model addition = constructQuery(SPARQL_PATH + "positions-addedStatements.rq", model, "", null);
        model.add(addition);
        model.remove(constructQuery(SPARQL_PATH + "positions-removedStatements.rq", model, "", null));
        return model;
    }

    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        try {
            return new VivoModelIterator();
        } catch (Exception e) {
            log.error(e, e);
            throw new RuntimeException(e);
        }
    }
    
    private class VivoModelIterator implements IteratorWithSize<Model> {

        private Set<String> searchResults = new HashSet<String>();
        private Iterator<String> searchResultIterator;
        
        private Set<String> retrievedURIs = new HashSet<String>();
        private Map<String, Model> lodModelCache = new HashMap<String, Model>();
        private Map<String, Model> resourceTypeCache = new HashMap<String, Model>();
        
        public VivoModelIterator() throws IOException, URISyntaxException {
            for (String filterTerm : getConfiguration().getQueryTerms()) {
                searchResults.addAll(getUrisFromSearchResults(
                        getRemoteVivoURL(), filterTerm, VivoVocabulary.CLASSGROUP_ACTIVITIES));
                searchResults.addAll(getUrisFromSearchResults(
                        getRemoteVivoURL(), filterTerm, VivoVocabulary.CLASSGROUP_RESEARCH));
            }
            searchResultIterator = searchResults.iterator();
            log.debug("Finished constructing model iterator");
        }
        
        public boolean hasNext() {
            return searchResultIterator.hasNext();
        }

        public Model next() {
            log.debug("*********************************************** NEXT **");
            String uri = searchResultIterator.next();
            Model uriModel = (fetchLOD(uri));
            if (isDocument(uri, uriModel)) {
               log.debug("Adding persons to document");                
                Model authorsModel = addRelatedResources(fetchRelatedResources(
                        uriModel, VivoVocabulary.AUTHORSHIP), 
                        VivoVocabulary.PERSON);
                authorsModel.add(fetchPersonPubsAndAffiliations(authorsModel));
                uriModel.add(authorsModel);
            } else if(isProject(uri, uriModel) || isGrant(uri, uriModel)) {
                log.debug("Adding stuff to grant/project.");
                // get persons related to grants/projects
                Model roleModel = fetchRelatedResources(uriModel,
                        VivoVocabulary.ROLE);
                roleModel.add(fetchRelatedResources(uriModel, 
                        VivoVocabulary.OLD_ROLE));
                Model relevantPersonModel = fetchRelatedResources(
                        roleModel, VivoVocabulary.PERSON);
                Model relevantOrganizationModel = fetchRelatedResources(
                        roleModel, VivoVocabulary.ORGANIZATION);
                uriModel.add(fetchPersonPubsAndAffiliations(relevantPersonModel));
                uriModel.add(roleModel);
                uriModel.add(relevantPersonModel);
                uriModel.add(relevantOrganizationModel);
            }
            log.debug("Adding date/time intervals");
            uriModel.add(addRelatedResources(fetchRelatedResources(
                    uriModel, VivoVocabulary.DATETIME_INTERVAL)));            
            // add any DateTimeValues
            log.debug("Adding date/time values");
            uriModel.add(addRelatedResources(
                    uriModel, VivoVocabulary.DATETIME_VALUE));            
            // add any skos:Concepts
            log.debug("Adding concepts");
            uriModel.add(fetchRelatedResources(
                    uriModel, VivoVocabulary.CONCEPT));
            // add any Journals
            log.debug("Adding journals");
            uriModel.add(fetchRelatedResources(
                    uriModel, VivoVocabulary.JOURNAL));
            // get any vCards we can
            log.debug("Adding vcards");
            uriModel.add(addRelatedResources(fetchRelatedResources(
                    uriModel, VivoVocabulary.VCARD_KIND)));
            log.debug("Adding addresses");
            // any pre-ISF addresses we can
            uriModel.add(addRelatedResources(fetchRelatedResources(
                    uriModel, VivoVocabulary.OLD_ADDRESS)));
            // we want to link positions to their top-level orgs to avoid
            // filling the repository with confusing university-specific
            // department names
            // get orgs related to grants/projects
            log.debug("Adding orgs");
            uriModel.add(fetchRelatedResources(uriModel, 
                    VivoVocabulary.ORGANIZATION));
            log.debug("Adding ancestry");
            uriModel.add(organizationAncestry(uriModel));
            return uriModel;
        }

        private Model fetchPersonPubsAndAffiliations(Model relevantPersonModel) {
            // For now, just get affiliations
            return addRelatedResources(fetchRelatedResources(
                  relevantPersonModel, VivoVocabulary.POSITION), 
                  VivoVocabulary.ORGANIZATION);
            // Retrieving all the publications and their authors takes forever
//            Model model = ModelFactory.createDefaultModel();
//            Model authorshipModel = fetchRelatedResources(
//                    relevantPersonModel, VivoVocabulary.AUTHORSHIP);
//            Model publicationModel = fetchRelatedResources(
//                    authorshipModel, VivoVocabulary.DOCUMENT);
//            publicationModel.add(addRelatedResources(fetchRelatedResources(
//                    publicationModel, VivoVocabulary.AUTHORSHIP), 
//                    VivoVocabulary.PERSON));
//            publicationModel.add(addRelatedResources(fetchRelatedResources(
//                    publicationModel, VivoVocabulary.POSITION), 
//                    VivoVocabulary.ORGANIZATION));
//            model.add(authorshipModel);
//            model.add(publicationModel);
//            return model;
        }
        
        public Integer size() {
            return searchResults.size();
        }
        
        /**
         * Return all parent organizations for organizations found in the
         * given model 
         * @param model
         * @return
         */
        protected Model organizationAncestry(Model model) {
            Set<String> visitedURIs = new HashSet<String>();
            return organizationAncestry(model, visitedURIs);
        }
        
        private Model organizationAncestry(Model model, Set<String> visitedURIs) {
            Model ancestors = ModelFactory.createDefaultModel();
            ResIterator rit = model.listSubjectsWithProperty(
                    RDF.type, VivoVocabulary.ORGANIZATION);
            while(rit.hasNext()){
                Resource r = rit.next();
                ancestors.add(parentOrgs(r, 
                        VivoVocabulary.PART_OF, model, visitedURIs));
                ancestors.add(parentOrgs(r, 
                        VivoVocabulary.OLD_SUBORG_WITHIN, model, visitedURIs));
            }
            return ancestors;
        }
        
        private Model parentOrgs(Resource org, Property subOrgProperty, 
                Model model, Set<String> visitedURIs) {
            Model parentOrgs = ModelFactory.createDefaultModel();            
            NodeIterator parentIt = model.listObjectsOfProperty(
                    org, subOrgProperty);
            while(parentIt.hasNext()) {
                RDFNode parent = parentIt.next();    
                if(!parent.isURIResource()) {
                    continue;
                }
                if(visitedURIs.contains(parent.asResource().getURI())) {
                    continue;
                }
                visitedURIs.add(parent.asResource().getURI());
                Model orgModel = fetchLOD(parent.asResource().getURI(), 
                        PROCEED_WITH_REPEAT_VISIT);
                parentOrgs.add(orgModel);
                parentOrgs.add(organizationAncestry(orgModel, visitedURIs));
            }
            return parentOrgs;            
        }
        
        /**
         * Fetch the model returned by a linked data request for a given URI
         * @param uri
         * @return
         */
        private Model fetchLOD(String uri) {
            return fetchLOD(uri, !PROCEED_WITH_REPEAT_VISIT);
        }
        
        private static final boolean PROCEED_WITH_REPEAT_VISIT = true;
        
        /**
         * Fetch the model returned by a linked data request for a given URI
         * @param uri
         * @param proceedWithRepeatVisit - true if fetch should proceed even if 
         * URI has already been visited
         * @return
         */
        private Model fetchLOD(String uri, boolean proceedWithRepeatVisit) {
            Model lodModel = ModelFactory.createDefaultModel();
            if(uri == null) {
                return lodModel;
            }
            if(proceedWithRepeatVisit || !retrievedURIs.contains(uri)) {
                if(lodModelCache.keySet().contains(uri)) {
                    log.debug("Returning " + uri + " from cache");
                    return clone(lodModelCache.get(uri));
                }
                log.info("Fetching " + uri);
                retrievedURIs.add(uri);
                try {
                    lodModel.read(uri);
                    lodModel.add(getMissingTypes(lodModel));
                } catch (RiotNotFoundException e) {
                    log.warn("Linked data resource not found: " + uri);
                    tryAlternative(uri, lodModel);
                } catch (RiotException e) {
                    if(uri.startsWith(MESH)) {
                        log.warn("Exception reading " + uri);
                    } else {
                        log.warn("Exception reading " + uri, e);
                    }
                    tryAlternative(uri, lodModel);
                }
                if(shouldCache(uri, lodModel)) {
                    lodModelCache.put(uri, clone(lodModel));
                }
                try {
                    Thread.sleep(MIN_REST);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }            
            }   
            return lodModel;
        }
        
        /*
         * The VIVO connector relies on having the types of related individuals
         * in object returned with the linked data request for a given subject.
         * This is not (necessarily) the case with pre-1.6 VIVOs.  So, to be 
         * sure, we will look for any resources in object position (that are 
         * not the object of rdf:type), issue a request for those, and add any
         * triples with the predicate rdf:type to the original model.
         */
        private Model getMissingTypes(Model lodModel) {
            Model missingTypes = ModelFactory.createDefaultModel();
            String query = "PREFIX rdf: <" + RDF.getURI() + "> \n" +
                           "SELECT DISTINCT ?o WHERE { \n" +
                           "  ?s ?p ?o . \n" +
                           "  FILTER(isURI(?o)) \n" +
                           "  FILTER(?p != rdf:type) \n" +
                           "  FILTER NOT EXISTS { \n" +
                           "    ?o a ?something \n" +
                           "  } \n" +
                           "} \n";
            QueryExecution qe = QueryExecutionFactory.create(query, lodModel);
            try {
                ResultSet rs = qe.execSelect();
                while(rs.hasNext()) {
                    QuerySolution qsoln = rs.next();
                    Resource res = qsoln.getResource("o");
                    String uri = res.getURI();
                    Model tmp = ModelFactory.createDefaultModel();
                    if(resourceTypeCache.keySet().contains(uri)) {
                        log.debug("Retrieving " + uri + " from types cache");
                        missingTypes.add(clone(resourceTypeCache.get(uri)));
                    } else {
                        log.info("Reading URI " + uri);
                        try {
                            tmp.read(uri);
                        } catch (Exception e) {
                            log.debug("Unable to add types for " + uri);
                            log.debug(e, e);
                        }
                    }
                    Model typesOnly = ModelFactory.createDefaultModel();
                    StmtIterator sit = tmp.listStatements(res, RDF.type, (Resource) null);
                    while(sit.hasNext()) {                        
                        Statement stmt = sit.next();
                        typesOnly.add(stmt);
                    }
                    resourceTypeCache.put(uri, clone(typesOnly));
                    missingTypes.add(typesOnly);
                }
            } finally {
                if(qe != null) {
                    qe.close();
                }
            }
            if(missingTypes.size() > 0) {
                log.debug("Adding " + missingTypes.size() + " missing type statements");
            }
            return missingTypes;
        }
        
        private void tryAlternative(String uri, Model model) {
            try {
                if(uri.startsWith(MESH)) {
                    log.info("Fetching alternative address " + uri + ".n3");
                    model.read(uri + ".n3");
                }
            } catch (Exception e) {
                log.debug("Exception attempting alternative requests for " 
                        + uri, e);
            }
        }
        
        protected Model clone(Model model) {
            Model clone = ModelFactory.createDefaultModel();
            clone.add(model);
            return clone;
        }
        
        protected boolean shouldCache(String uri, Model lodModelForUri) {
            return lodModelForUri.contains(
                    lodModelForUri.getResource(uri), 
                    RDF.type, VivoVocabulary.ORGANIZATION);
        }

        /**
         * Fetch resources related to the resources in the given model
         * @param model containing original data
         * @param type of related resources to be fetched (optional; may be null)
         * @throws InterruptedException
         */
        protected Model fetchRelatedResources(Model model, Resource type) {
            Model related = ModelFactory.createDefaultModel();
            ResIterator nit = model.listResourcesWithProperty(RDF.type, type);
            if(type != null) {
                log.debug("Listing resources with type <" + type.getURI() + ">");
            }
            while(nit.hasNext()) {
                Resource n = nit.next();
                if(n.isURIResource()) {                    
                    Resource r = n.asResource();
                    log.debug("Found" + r.getURI());
                    if(isIrrelevantResource(r, model)) {
                        continue;
                    }
                    try {
                        log.debug("Fetching related resource " + r.getURI());
                        if(VivoVocabulary.ORGANIZATION.equals(type)) {
                            related.add(fetchLOD(r.getURI(), PROCEED_WITH_REPEAT_VISIT));
                        } else {
                            related.add(fetchLOD(r.getURI()));
                        }
                    } catch (Exception e) {
                        log.error("Error retrieving " + r.getURI(), e);
                    }
                }
            }
            if(related.isEmpty()) {
                if(type != null) {
                    log.debug("Found no type " + type.getURI());
                } else {
                    log.debug("Found no related resources ");
                }
                if(log.isDebugEnabled()) {
                    StringWriter sw = new StringWriter();
                    model.write(sw, "TTL");
                    log.debug("... in model " + sw.toString());
                }
            }
            return related;
        }
        
        private boolean isIrrelevantResource(Resource r, Model model) {
            // don't go off site
            if(r.getURI().startsWith(VivoVocabulary.VIVO)
                    || r.getURI().startsWith(VivoVocabulary.FOAF)
                    || r.getURI().startsWith(VivoVocabulary.BIBO)
                    || r.getURI().startsWith(VivoVocabulary.VCARD)
                    || r.getURI().startsWith(VivoVocabulary.OBO)
                    || r.getURI().startsWith(VivoVocabulary.SKOS)) {
                return true;
            }
            // don't try to retrieve classes
            if(model.contains(null, RDF.type, r)) {
                return true;
            }
            if(model.contains(r, RDF.type, OWL.Class) 
                    || model.contains(r, RDF.type, OWL.ObjectProperty)
                    || model.contains(r, RDF.type, OWL.DatatypeProperty)
                    || model.contains(r, RDF.type, OWL.AnnotationProperty)
                    || model.contains(r, RDF.type, RDF.Property)
                    ) {
                return true;
            }
            return false;
        }

        /**
         * Add resources related to the resources in the given model
         * and return a model containing the original plus the added data
         * @param model containing original data
         * @param type of related resources to be fetched (optional; may be null)
         * @return copy of original model with added data, for daisy chaining
         * @throws InterruptedException
         */
        protected Model addRelatedResources(Model model, Resource type) {
            Model m = clone(model).add(fetchRelatedResources(model, type));
            return m;
        }

        
        /**
         * Add resources related to the resources in the given model
         * and return a model containing the original plus the added data
         * @param model containing original data
         * @return copy of original model with added data, for daisy chaining
         * @throws InterruptedException
         */
        protected Model addRelatedResources(Model model) {
            model.add(fetchRelatedResources(model));
            return model;
        }
        
        /**
         * Fetch resources related to the resources in the given model
         * @param model containing original data
         * @throws InterruptedException
         */
        protected Model fetchRelatedResources(Model model) {
            return fetchRelatedResources(model, null);
        }

        private boolean isDocument(String uri, Model uriModel) {
            return uriModel.contains(
                    uriModel.getResource(uri), RDF.type, VivoVocabulary.DOCUMENT);
        }

        private boolean isGrant(String uri, Model uriModel) {
            return uriModel.contains(
                    uriModel.getResource(uri), RDF.type, VivoVocabulary.GRANT);
        }
        
        private boolean isProject(String uri, Model uriModel) {
            return uriModel.contains(
                    uriModel.getResource(uri), RDF.type, VivoVocabulary.PROJECT);
        }
        
    }
    
}
