package org.wheatinitiative.vivo.datasource.connector;

import java.util.Arrays;
import java.util.List;

import org.wheatinitiative.vivo.datasource.connector.rcuk.Rcuk;

import junit.framework.Test;
import junit.framework.TestSuite;

public class RcukTest extends ConnectorTestCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public RcukTest( String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite( RcukTest.class );
    }

    /**
     * Test that the configuration getter/setters work properly
     */
    public void testConfiguration()
    {
        List<String> queryTerms = Arrays.asList("wheat", "cheese", "grâu", "brânzã");
        Rcuk rcuk = new Rcuk();
        rcuk.getConfiguration().getParameterMap().put("queryTerms", queryTerms);
        assertEquals(queryTerms, rcuk.getConfiguration().getParameterMap().get("queryTerms"));
    }
    
}
