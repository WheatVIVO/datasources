package org.wheatinitiative.vivo.datasource.connector.usda;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vitro.webapp.ontology.update.KnowledgeBaseUpdater;
import org.wheatinitiative.vitro.webapp.ontology.update.UpdateSettings;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;
import org.wheatinitiative.vivo.datasource.util.classpath.ClasspathUtils;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

public class Usda extends VivoDataSource implements DataSource {

    private static final String ENDPOINT_URL = "http://vivo.usda.gov";
    private static final String PEOPLE = 
            "http://vivoweb.org/ontology#vitroClassGrouppeople";
    private final static int MIN_REST = 300; // ms between linked data requests
    private static final String RESOURCE_PATH = "/vivo/update15to16/"; 
    private Log log = LogFactory.getLog(Usda.class);
    
    private static final int LIMIT = 99999; // max search results to retrieve
    
    @Override
    public void runIngest() {
        Model resultModel = ModelFactory.createDefaultModel();
        try {
            Set<String> uris = new HashSet<String>();
            for (String filterTerm : this.getConfiguration().getQueryTerms()) {
                uris.addAll(getUrisFromSearchResults(ENDPOINT_URL, filterTerm, 
                        PEOPLE));
                int limit = LIMIT;
                for(String uri : uris) {
                    limit--;
                    if (limit < 0) {
                        break;
                    }
                    //Model m = httpUtils.getRDFLinkedDataResponse(uri);
                    Model m = ModelFactory.createDefaultModel();
                    log.info("Fetching search result " + uri);
                    m.read(uri);
                    resultModel.add(m);
                }
                Thread.sleep(MIN_REST);
            }
            fetchRelatedURIs(resultModel);
            resultModel = updateToOneSix(resultModel);
        } catch (Exception e) {
            log.error(e, e);
        } finally {
            this.result = resultModel;
        }
    }
    
    private void fetchRelatedURIs(Model model) throws InterruptedException {
        Model tmp = ModelFactory.createDefaultModel();
        NodeIterator nit = model.listObjects();
        while(nit.hasNext()) {
            RDFNode n = nit.next();
            if(n.isURIResource()) {
                Resource r = n.asResource();
                if(model.contains(r, RDF.type, (Resource) null)) {
                    continue;
                }
                if(model.contains(null, RDF.type, r)) {
                    continue;
                }
                try {
                    log.info("Fetching related resource " + r.getURI());
                    tmp.read(r.getURI());
                } catch (Exception e) {
                    log.error("Error retreiving " + r.getURI());
                }
            }
            Thread.sleep(MIN_REST);
        }
        model.add(tmp);
    }

    private Model updateToOneSix(Model model) {
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
    
}
