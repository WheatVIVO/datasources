package org.wheatinitiative.vivo.datasource.publish;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import junit.framework.TestCase;

public class PublisherTest extends TestCase {

    protected Set<String> functionalPropertyURIs = new HashSet<String>(
            Arrays.asList(RDFS.label.getURI()));
    protected String graph1 = "http://example.com/graph1";
    protected String graph2 = "http://example.com/graph2";
    protected List<String> graphURIPreferenceList = Arrays.asList(
            graph1, graph2);
    protected Resource resource1 = ResourceFactory.createResource(
            "http://example.com/individual1");
    protected Resource resource2 = ResourceFactory.createResource(
            "http://example.com/individual2");
    
    private class TestablePublisher extends Publisher {
        
        public void dedupFunctionalProperties(Map<String, Model> quadStore, 
                Set<String> functionalPropertyURIs, 
                List<String> graphURIPreferenceList) {
            super.dedupFunctionalProperties(
                    quadStore, functionalPropertyURIs, graphURIPreferenceList);
        }
        
    }
    
    public void testDuplicateLabelSameGraph() {
        Map<String, Model> quadStore = new HashMap<String, Model>();
        Model model1 = ModelFactory.createDefaultModel();
        model1.add(resource1, RDFS.label, "Cheese, Chuck");
        model1.add(resource1, RDFS.label, "Cheese, Charles");
        quadStore.put(graph1, model1);
        Model correct = ModelFactory.createDefaultModel();
        correct.add(resource1, RDFS.label, "Cheese, Charles");
        TestablePublisher publisher = new TestablePublisher();
        publisher.dedupFunctionalProperties(quadStore, functionalPropertyURIs, 
                graphURIPreferenceList);
        assertEquals(1, model1.size());
        assertTrue("models not isomorphic (expected: " 
                + correct + ", actual: " + model1 +")", 
                model1.isIsomorphicWith(correct));
    }
    
    public void testSingleLabelSingleGraph() {
        Map<String, Model> quadStore = new HashMap<String, Model>();
        Model model1 = ModelFactory.createDefaultModel();
        model1.add(resource1, RDFS.label, "Cheese, Charles");
        quadStore.put(graph1, model1);
        Model correct = ModelFactory.createDefaultModel();
        correct.add(resource1, RDFS.label, "Cheese, Charles");
        TestablePublisher publisher = new TestablePublisher();
        publisher.dedupFunctionalProperties(quadStore, functionalPropertyURIs, 
                graphURIPreferenceList);
        assertEquals(1, model1.size());
        assertTrue("models not isomorphic (expected: " 
                + correct + ", actual: " + model1 +")", 
                model1.isIsomorphicWith(correct));
    }
    
    public void testDuplicateLabelsDifferentGraphs() {
        Map<String, Model> quadStore = new HashMap<String, Model>();
        Model model1 = ModelFactory.createDefaultModel();
        Model model2 = ModelFactory.createDefaultModel();
        model1.add(resource1, RDFS.label, "Cheese, Chuck");
        model1.add(resource1, RDFS.label, "Cheese, Charles");
        model2.add(resource1, RDFS.label, "Cheese, Charles");
        quadStore.put(graph1, model1);
        quadStore.put(graph2, model2);
        Model correctModel1 = ModelFactory.createDefaultModel();
        correctModel1.add(resource1, RDFS.label, "Cheese, Charles");
        TestablePublisher publisher = new TestablePublisher();
        publisher.dedupFunctionalProperties(quadStore, functionalPropertyURIs, 
                graphURIPreferenceList);
        assertEquals(1, model1.size());
        assertEquals(0, model2.size());
        assertTrue("models not isomorphic (expected: " 
                + correctModel1 + ", actual: " + model1 +")", 
                model1.isIsomorphicWith(correctModel1));
    }
    
    public void testDuplicateLabelsSecondGraphOnly() {
        Map<String, Model> quadStore = new HashMap<String, Model>();
        Model model1 = ModelFactory.createDefaultModel();
        Model model2 = ModelFactory.createDefaultModel();
        model1.add(resource1, RDF.type, OWL.Thing);
        model2.add(resource1, RDFS.label, "Cheese, Charles");
        model2.add(resource1, RDFS.label, "Cheese, Chuck");
        quadStore.put(graph1, model1);
        quadStore.put(graph2, model2);
        Model correctModel1 = ModelFactory.createDefaultModel();
        correctModel1.add(resource1, RDF.type, OWL.Thing);
        Model correctModel2 = ModelFactory.createDefaultModel();
        correctModel2.add(resource1, RDFS.label, "Cheese, Charles");
        TestablePublisher publisher = new TestablePublisher();
        publisher.dedupFunctionalProperties(quadStore, functionalPropertyURIs, 
                graphURIPreferenceList);
        assertEquals(1, model1.size());
        assertEquals(1, model2.size());
        assertTrue("models not isomorphic (expected: " 
                + correctModel1 + ", actual: " + model1 +")", 
                model1.isIsomorphicWith(correctModel1));
        assertTrue("models not isomorphic (expected: " 
                + correctModel2 + ", actual: " + model2 +")", 
                model2.isIsomorphicWith(correctModel2));
    }
    
}
