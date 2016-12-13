package edu.cornell.mannlib.vitro.webapp.ontology.update;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import junit.framework.TestCase;

public class KnowledgeBaseUpdaterTest extends TestCase {

    private static final String RESOURCE_PATH = "/vivo/update15to16/"; 
    
    private URI getResourceURI(String relativePath) {
        try {
            URL url = this.getClass().getResource(
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
    
    public void testOneFiveToOneSix() {
        String onefive = "@prefix vivo: <http://vivoweb.org/ontology/core#> . \n" +
            "vivo:n1 a vivo:Position . \n" +
            "vivo:n2 vivo:personInPosition vivo:n1 . \n" +
            "vivo:n1 vivo:positionInOrganization vivo:n3 . \n";
        String onesix = "@prefix vivo: <http://vivoweb.org/ontology/core#> . \n" +
                "vivo:n1 a vivo:Position . \n" +
                "vivo:n2 vivo:relatedBy vivo:n1 . \n" +
                "vivo:n1 vivo:relates vivo:n3 . \n";
        UpdateSettings settings = new UpdateSettings();
        Model abox = ModelFactory.createDefaultModel();
        abox.read(new ByteArrayInputStream(onefive.getBytes()), null, "N3");
        settings.setABoxModel(abox);
        settings.setDefaultNamespace("http://vivo.example.org/individual/");
        settings.setDiffFile(getResourceURI("diff.tab.txt"));
        settings.setSparqlConstructAdditionsDir(getResourceURI(
                "sparqlConstructs/additions"));
        settings.setSparqlConstructDeletionsDir(getResourceURI(
                "sparqlConstructs/deletions"));
        OntModel oldTBoxModel = ModelFactory.createOntologyModel(
                OntModelSpec.OWL_MEM);
        URI oldTBoxDirURI = getResourceURI("oldVersion");
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
        Model onesixModel = ModelFactory.createDefaultModel();
        onesixModel.read(new ByteArrayInputStream(
                onesix.getBytes()), null, "N3");
        assertTrue(onesixModel.isIsomorphicWith(abox));
    }
    
}
