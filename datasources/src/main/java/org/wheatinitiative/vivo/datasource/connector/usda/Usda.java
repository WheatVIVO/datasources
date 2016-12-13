package org.wheatinitiative.vivo.datasource.connector.usda;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

import edu.cornell.mannlib.vitro.webapp.ontology.update.KnowledgeBaseUpdater;
import edu.cornell.mannlib.vitro.webapp.ontology.update.UpdateSettings;

public class Usda extends VivoDataSource implements DataSource {

    private static final String ENDPOINT_URL = "http://vivo.usda.gov";
    private static final String PEOPLE = 
            "http://vivoweb.org/ontology#vitroClassGrouppeople";
    private final static int MIN_REST = 300; // ms between linked data requests
    private static final String RESOURCE_PATH = "/vivo/update15to16/"; 
    private Log log = LogFactory.getLog(Usda.class);
    
    private static final int LIMIT = 100000; // max search results to retrieve
    
    public Usda(List<String> filterTerms) {
        super(filterTerms);
    }
    
    @Override
    public void run() {
        Model resultModel = ModelFactory.createDefaultModel();
        try {
            Set<String> uris = new HashSet<String>();
            for (String filterTerm : filterTerms) {
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
        settings.setDiffFile(getResourceURI("diff.tab.txt"));
        settings.setSparqlConstructAdditionsDir(getResourceURI(
                "sparqlConstructs/additions"));
        settings.setSparqlConstructDeletionsDir(getResourceURI(
                "sparqlConstructs/deletions"));
        OntModel oldTBoxModel = ModelFactory.createOntologyModel(
                OntModelSpec.OWL_MEM);
        URI oldTBoxDirURI = getResourceURI("oldVersion");
        log.info(oldTBoxDirURI.toString());
        File oldTBoxDir = new File(oldTBoxDirURI);
        File[] files = oldTBoxDir.listFiles();
        for(int i = 0; i < files.length; i++) {
            try {
                FileInputStream fis = new FileInputStream(files[i]);
                oldTBoxModel.read(fis, null, "RDF/XML");
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
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
    
    
    private URI getResourceURI(String relativePath) {
        try {
            URL url = Usda.class.getResource(
                RESOURCE_PATH + relativePath);
            if (url == null) {
                return null;
            } else {
                return url.toURI();
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    
}
