package edu.cornell.mannlib.vitro.webapp.ontology.update;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.wheatinitiative.vivo.datasource.util.classpath.ClasspathUtils;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import junit.framework.TestCase;

public class KnowledgeBaseUpdaterTest extends TestCase {

    private static final String RESOURCE_PATH = "/vivo/update15to16/"; 
    
    private String resolveResource(String relativePath) {
        return RESOURCE_PATH + relativePath;
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
        Model onesixModel = ModelFactory.createDefaultModel();
        onesixModel.read(new ByteArrayInputStream(
                onesix.getBytes()), null, "N3");
        assertTrue(onesixModel.isIsomorphicWith(abox));
    }
    
}
