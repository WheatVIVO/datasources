package org.wheatinitiative.vivo.datasource.publish;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wheatinitiative.vivo.datasource.SparqlEndpointParams;
import org.wheatinitiative.vivo.datasource.connector.ConnectorTestCase;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import junit.framework.Assert;

public class PublisherTest extends ConnectorTestCase {

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

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public PublisherTest( String testName ) {
        super( testName );
    }
    
    private class TestablePublisher extends Publisher {

        public void dedupFunctionalProperties(Map<String, Model> quadStore, 
                Set<String> functionalPropertyURIs, 
                List<String> graphURIPreferenceList) {
            super.dedupFunctionalProperties(
                    quadStore, functionalPropertyURIs, graphURIPreferenceList);
        }

    }
    
    public void testPublish() {
        String wheatInitiativeGraph = "http://vitro.mannlib.cornell.edu/a/graph/wheatinitiative";
        String orcidGraph = "http://vitro.mannlib.cornell.edu/a/graph/ORCID";
        Dataset fromDataset = DatasetFactory.createMem();
        Dataset toDataset = DatasetFactory.createMem();
        Model dataSources = fromDataset.getNamedModel("http://vitro.mannlib.cornell.edu/filegraph/abox/adminapp-dataSources.n3");
        dataSources.add(loadModelFromResource("/publisher/dataSources.n3"));
        fromDataset.getDefaultModel().add(dataSources);
        Model orcid = fromDataset.getNamedModel(orcidGraph);
        orcid.add(loadModelFromResource("/publisher/orcid.n3"));
        Model wheatinitiative = fromDataset.getNamedModel(wheatInitiativeGraph);
        wheatinitiative.add(loadModelFromResource("/publisher/wheatinitiative.n3"));
        Model kbinf = fromDataset.getNamedModel("http://vitro.mannlib.cornell.edu/default/vitro-kb-inf");
        kbinf.add(loadModelFromResource("/publisher/kb-inf.n3"));
        fromDataset.getDefaultModel().add(kbinf);
        Publisher publisher = new InMemoryPublisher(fromDataset, toDataset);
        publisher.runIngest();
        String publishedWheatinitiativeFile = "/publisher/publishedWheatinitiative.n3";
        String publishedOrcidFile = "/publisher/publishedOrcid.n3";
        Model publishedWheatinitiative = loadModelFromResource(publishedWheatinitiativeFile);
        Model publishedOrcid = loadModelFromResource(publishedOrcidFile);
        assertTrue("contents of " + wheatInitiativeGraph + " do not match " + publishedWheatinitiativeFile, 
                toDataset.getNamedModel(wheatInitiativeGraph).isIsomorphicWith(publishedWheatinitiative));
        assertTrue("contents of " + orcidGraph + " do not match " + publishedOrcidFile, 
                toDataset.getNamedModel(orcidGraph).isIsomorphicWith(publishedOrcid));
        assertEquals(18, toDataset.getNamedModel("http://vitro.mannlib.cornell.edu/a/graph/wheatinitiative").size());
        assertEquals(31, toDataset.getNamedModel("http://vitro.mannlib.cornell.edu/a/graph/ORCID").size());
    }
    
    private class InMemoryPublisher extends Publisher {

        private SparqlEndpoint sourceEndpoint;
        private SparqlEndpoint destinationEndpoint;
        
        public InMemoryPublisher(Dataset fromDataset, Dataset toDataset) {
            this.sourceEndpoint = new DatasetSparqlEndpoint(fromDataset);
            this.destinationEndpoint = new DatasetSparqlEndpoint(toDataset);
        }
        
        @Override
        protected SparqlEndpoint getSourceEndpoint() {
            return this.sourceEndpoint;
        }
        
        @Override
        protected SparqlEndpoint getSparqlEndpoint() {
            return this.destinationEndpoint;
        }
        
        private class DatasetSparqlEndpoint extends SparqlEndpoint {
            
            private Dataset dataset;
            
            public DatasetSparqlEndpoint(Dataset dataset) {
                super(new SparqlEndpointParams());
                this.dataset = dataset;
            }
            
            @Override 
            public ResultSet getResultSet(String query) {
                QueryExecution qe = QueryExecutionFactory.create(query, dataset);
                try {
                    ResultSet rs = qe.execSelect();
                    return ResultSetFactory.copyResults(rs);
                } finally {
                    qe.close();    
                }
            }
            
            @Override
            public Model construct(String query) {
                QueryExecution qe = QueryExecutionFactory.create(query, dataset);
                try {
                    return qe.execConstruct();
                } finally {
                    qe.close();    
                }               
            }
            
            @Override
            public void clearGraph(String graphURI) {
                dataset.getNamedModel(graphURI).removeAll();
            }
            
            @Override
            public void writeModel(Model m, String graphURI) {
                dataset.getNamedModel(graphURI).add(m);
            }
            
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
