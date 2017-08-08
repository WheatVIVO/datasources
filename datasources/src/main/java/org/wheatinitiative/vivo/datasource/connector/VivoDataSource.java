package org.wheatinitiative.vivo.datasource.connector;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;
import org.wheatinitiative.vitro.webapp.ontology.update.KnowledgeBaseUpdater;
import org.wheatinitiative.vitro.webapp.ontology.update.UpdateSettings;
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
import com.hp.hpl.jena.vocabulary.RDF;

public class VivoDataSource extends ConnectorDataSource {

    private static final String SEARCH_CONTROLLER = "/search";
    private static final String QUERYTEXT_PARAM = "querytext";
    private static final String XML_PARAM = "xml";
    private static final String XML_VALUE = "1";
    private static final String HITS_PER_PAGE_PARAM = "hitsPerPage";
    private static final String HITS_PER_PAGE = "100";
    private static final String CLASSGROUP_PARAM = "classgroup";
    
    private Log log = LogFactory.getLog(VivoDataSource.class);
    protected Model result;
    protected HttpUtils httpUtils = new HttpUtils();
    protected XmlToRdf xmlToRdf = new XmlToRdf();

    private static final String PEOPLE = 
            "http://vivoweb.org/ontology#vitroClassGrouppeople";
    private final static int MIN_REST = 125; // ms between linked data requests
    private static final String RESOURCE_PATH = "/vivo/update15to16/"; 
    
    private Set<String> retrievedURIs = new HashSet<String>();
    
    protected String getRemoteVivoURL() {
        return this.getConfiguration().getServiceURI();
    }
    
    protected Model mapToVIVO(Model model) {
        // no work required in standard (VIVO 1.6+) VIVO data source
        return model;
    }
    
    /**
     * 
     * @param model
     * @throws InterruptedException
     */
    protected void fetchRelatedURIs(Model model) {
        Model tmp = ModelFactory.createDefaultModel();
        NodeIterator nit = model.listObjects();
        while(nit.hasNext()) {
            RDFNode n = nit.next();
            if(n.isURIResource()) {
                Resource r = n.asResource();
//                if(model.contains(r, RDF.type, (Resource) null)) {
//                    continue;
//                }
                // don't try to retrieve classes
                if(model.contains(null, RDF.type, r)) {
                    continue;
                }
                try {
                    log.info("Fetching related resource " + r.getURI());
                    fetch(r.getURI(), tmp);
                } catch (Exception e) {
                    log.error("Error retreiving " + r.getURI());
                }
            }
        }
        model.add(tmp);
    }
    
    private void fetch(String uri, Model model) {
        if(uri == null) {
            return;
        }
        if(!retrievedURIs.contains(uri)) {
            log.info("Fetching " + uri);
            retrievedURIs.add(uri);
            model.read(uri);
            try {
                Thread.sleep(MIN_REST);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }        
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
        settings.setNewTBoxModel(oldTBoxModel); // TODO doesn't matter
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
            log.debug("Requesting " + nextPageUrl.toString());
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
                            "for query text " + querytext);
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
        
        public VivoModelIterator() throws IOException, URISyntaxException {
            for (String filterTerm : getConfiguration().getQueryTerms()) {
                searchResults.addAll(getUrisFromSearchResults(
                        getRemoteVivoURL(), filterTerm, PEOPLE));
            }
            searchResultIterator = searchResults.iterator();
        }
        
        public boolean hasNext() {
            return searchResultIterator.hasNext();
        }

        public Model next() {
            Model model = ModelFactory.createDefaultModel();
            String uri = searchResultIterator.next();
            Model m = ModelFactory.createDefaultModel();
            fetch(uri, m);
            model.add(m);
            fetchRelatedURIs(model);
            return model;
        }

        public Integer size() {
            return searchResults.size();
        }
        
    }
    
}
