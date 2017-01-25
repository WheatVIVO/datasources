package org.wheatinitiative.vivo.datasource.connector.prodinra;

import org.wheatinitiative.vivo.datasource.connector.ConnectorTestCase;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
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

    /**
     * Test creation of publication type statements
     */
    public void testPublicationTypes()
    {
        Model in = this.loadModelFromResource("/prodinra/publicationTypesIn.n3");
        Model out = this.loadModelFromResource("/prodinra/publicationTypesOut.n3");
        TestProdinra prodinra = new TestProdinra();
        Model m = ModelFactory.createDefaultModel();
        m.add(in);
        long originalSize = m.size();
        m = prodinra.constructForVIVO(m);
        assertFalse("post-construct model is same size as original model", originalSize == m.size());
        StmtIterator sit = out.listStatements();
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            assertTrue("model does not contain " + stmt, m.contains(stmt));
        }
        Model union = ModelFactory.createUnion(in, out);
        assertTrue("expected " + union + " but was " + m, union.isIsomorphicWith(m));
    }
    
    // a subclass for testing protected methods
    private class TestProdinra extends Prodinra {
        
        public Model constructForVIVO(Model m) {
            return super.constructForVIVO(m);
        }
    
    }
    
}
