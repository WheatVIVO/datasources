package org.wheatinitiative.vivo.datasource.connector;

import java.io.IOException;
import java.io.InputStream;
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

    private static final String SEARCH_CONTROLLER = "/search";
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
    
    protected String getRemoteVivoURL() {
        return this.getConfiguration().getServiceURI();
    }
    
    protected Model mapToVIVO(Model model) {
        // no work required in standard (VIVO 1.6+) VIVO data source
        return model;
    }
    
    protected Model updateToOneSix(Model model) {
        UpdateSettings settings = new UpdateSettings();
        settings.setABoxModel(model);
        settings.setDefaultNamespace("http://vivo.example.org/individual/");
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
        URIBuilder builder = new URIBuilder(vivoUrl + SEARCH_CONTROLLER);
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
                URIBuilder nextPageBuilder = new URIBuilder(vivoUrl + nextPage);
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
        // nothing to do, for now
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
        
        public VivoModelIterator() throws IOException, URISyntaxException {
            for (String filterTerm : getConfiguration().getQueryTerms()) {
                searchResults.addAll(getUrisFromSearchResults(
                        getRemoteVivoURL(), filterTerm, VivoVocabulary.CLASSGROUP_ACTIVITIES));
                searchResults.addAll(getUrisFromSearchResults(
                        getRemoteVivoURL(), filterTerm, VivoVocabulary.CLASSGROUP_RESEARCH));
            }
            searchResultIterator = searchResults.iterator();
            log.info("Finished constructing model iterator");
        }
        
        public boolean hasNext() {
            return searchResultIterator.hasNext();
        }

        public Model next() {
            log.info("*********************************************** NEXT **");
            String uri = searchResultIterator.next();
            Model uriModel = (fetchLOD(uri));
            if (isDocument(uri, uriModel)) {
               log.info("Adding persons to document");                
                Model authorsModel = addRelatedResources(fetchRelatedResources(
                        uriModel, VivoVocabulary.AUTHORSHIP), 
                        VivoVocabulary.PERSON);
                authorsModel.add(addRelatedResources(fetchRelatedResources(
                        authorsModel, VivoVocabulary.POSITION), 
                        VivoVocabulary.ORGANIZATION));
                uriModel.add(authorsModel);
            } else if(isProject(uri, uriModel) || isGrant(uri, uriModel)) {
                log.info("Adding stuff to grant/project.  Need to go through role.");
                // get persons related to grants/projects
                Model roleModel = fetchRelatedResources(uriModel,
                        VivoVocabulary.ROLE);
                roleModel.add(fetchRelatedResources(uriModel, 
                        VivoVocabulary.OLD_ROLE));
                Model relevantPersonModel = fetchRelatedResources(
                        roleModel, VivoVocabulary.PERSON);
                Model authorshipModel = fetchRelatedResources(
                        relevantPersonModel, VivoVocabulary.AUTHORSHIP);
                Model publicationModel = fetchRelatedResources(
                        authorshipModel, VivoVocabulary.DOCUMENT);
                publicationModel.add(addRelatedResources(fetchRelatedResources(
                        publicationModel, VivoVocabulary.AUTHORSHIP), 
                        VivoVocabulary.PERSON));
                publicationModel.add(addRelatedResources(fetchRelatedResources(
                        publicationModel, VivoVocabulary.POSITION), 
                        VivoVocabulary.ORGANIZATION));
                uriModel.add(relevantPersonModel);
                uriModel.add(authorshipModel);
                uriModel.add(publicationModel);
            } 
            // add any DateTimeIntervals
            log.info("Adding date/time intervals");
            uriModel.add(addRelatedResources(fetchRelatedResources(
                    uriModel, VivoVocabulary.DATETIME_INTERVAL)));            
            // add any DateTimeValues
            log.info("Adding date/time values");
            uriModel.add(addRelatedResources(
                    uriModel, VivoVocabulary.DATETIME_VALUE));            
            // add any skos:Concepts
            log.info("Adding concepts");
            uriModel.add(fetchRelatedResources(
                    uriModel, VivoVocabulary.CONCEPT));
            // add any Journals
            log.info("Adding journals");
            uriModel.add(fetchRelatedResources(
                    uriModel, VivoVocabulary.JOURNAL));
            // get any vCards we can
            log.info("Adding vcards");
            uriModel.add(addRelatedResources(fetchRelatedResources(
                    uriModel, VivoVocabulary.VCARD_KIND)));
            log.info("Adding addresses");
            // any pre-ISF addresses we can
            uriModel.add(addRelatedResources(fetchRelatedResources(
                    uriModel, VivoVocabulary.OLD_ADDRESS)));
            // we want to link positions to their top-level orgs to avoid
            // filling the repository with confusing university-specific
            // department names
            // get orgs related to grants/projects
            log.info("Adding orgs");
            uriModel.add(fetchRelatedResources(uriModel, 
                    VivoVocabulary.ORGANIZATION));
            log.info("Adding ancestry");
            uriModel.add(organizationAncestry(uriModel));
            return uriModel;
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
        
        private Model parentOrgs(Resource org, Property subOrgProperty, Model model, Set<String> visitedURIs) {
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
         * @param proceedWithRepeatVisit - true if fetch should proceed even if uri
         * has already been visited
         * @return
         */
        private Model fetchLOD(String uri, boolean proceedWithRepeatVisit) {
            Model lodModel = ModelFactory.createDefaultModel();
            if(uri == null) {
                return lodModel;
            }
            if(proceedWithRepeatVisit || !retrievedURIs.contains(uri)) {
                if(lodModelCache.keySet().contains(uri)) {
                    log.info("Returning " + uri + " from cache");
                    return clone(lodModelCache.get(uri));
                }
                log.info("Fetching " + uri);
                retrievedURIs.add(uri);
                try {
                    lodModel.read(uri);
                } catch (RiotNotFoundException e) {
                    log.warn("Linked data resource not found: " + uri);
                    tryAlternative(uri, lodModel);
                } catch (RiotException e) {
                    log.warn("Exception reading " + uri, e);
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
        
        private void tryAlternative(String uri, Model model) {
            try {
                if(uri.startsWith(MESH)) {
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
                        related.add(fetchLOD(r.getURI()));
                    } catch (Exception e) {
                        log.error("Error retrieving " + r.getURI(), e);
                    }
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
