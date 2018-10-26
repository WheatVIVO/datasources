package org.wheatinitiative.vivo.datasource.connector.prodinra;

import java.util.Arrays;
import java.util.List;

import org.wheatinitiative.vivo.datasource.connector.ConnectorTestCase;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import junit.framework.Test;
import junit.framework.TestSuite;

public class ProdinraTest extends ConnectorTestCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ProdinraTest( String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite( ProdinraTest.class );
    }

    protected Model testConstruct(String startResource, String resultResource) {
        return testConstruct(startResource, resultResource, false);
    }
    
    /**
     * Test that a mapping rules run against the RDF statements in startResource
     * include all statements in endResource.  
     * @param startResource
     * @param endResource
     * @param checkIsomorphism - if true, check that the result of the mapping
     * is isomorphic with the union of startResource and endResource
     * @return constructed model for possible further analysis
     */
    protected Model testConstruct(String startResource, String endResource, 
            boolean checkIsomorphism) {
        Model in = this.loadModelFromResource(startResource);
        Model out = this.loadModelFromResource(endResource);
        TestProdinra prodinra = new TestProdinra();
        Model m = ModelFactory.createDefaultModel();
        m.add(in);
        long originalSize = m.size();
        m = prodinra.constructForVIVO(m);
        assertFalse("post-construct model is same size as original model",
                originalSize == m.size());
        StmtIterator sit = out.listStatements();
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            assertTrue("model does not contain " + stmt, m.contains(stmt));
        }
        if(checkIsomorphism) {
            Model union = ModelFactory.createUnion(in, out);
            assertTrue("Graph not isomporphic. Expected " + union + " but was " + m, 
                    union.isIsomorphicWith(m));
        }
        return m;
    }
    
    /**
     * Test creation of publication type statements
     */
    public void testPublicationTypes()
    {
        testConstruct("/prodinra/publicationTypesIn.n3",
                "/prodinra/publicationTypesOut.n3",
                true);
    }
    
    public void testAuthorships() {
        testConstruct("/prodinra/authorshipIn.n3", "/prodinra/authorshipOut.n3");
    }
    
    public void testAuthorLabel() {
        testConstruct("/prodinra/authorLabelIn.n3", "/prodinra/authorLabelOut.n3");
    }
    
    public void testAbstract() {
        testConstruct("/prodinra/abstractIn.n3", "/prodinra/abstractOut.n3");
    }
    
    public void testExternalAffiliation() {
        testConstruct("/prodinra/externalAffiliationIn.n3", 
                "/prodinra/externalAffiliationOut.n3");
    }
        
    public void testInraAffiliation() {
        testConstruct("/prodinra/inraAffiliationUnitIn.n3", 
                "/prodinra/inraAffiliationUnitOut.n3");
    }
    
    public void testFilter() {
        Model m = this.loadModelFromResource("/prodinra/inraAffiliationUnitIn.n3");
        TestProdinra prodinra = new TestProdinra();
        prodinra.getConfiguration().setQueryTerms(Arrays.asList("agricole"));
        m = prodinra.constructForVIVO(m);
        List<Resource> relevantResources = prodinra.getRelevantResources(m);
        assertEquals(2, relevantResources.size());
        long preFilterSize = m.size();
        m = prodinra.filter(m);
        assertTrue("filtered model is larger than or equal to the prefiltered model", 
                m.size() < preFilterSize);
        assertTrue("filtered model is empty", m.size() > 0);
    }
    
    // a subclass for testing protected methods
    private class TestProdinra extends Prodinra {
        
        public Model constructForVIVO(Model m) {
            return super.constructForVIVO(m);
        }
    
    }
    
}
