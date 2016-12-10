package org.wheatinitiative.vivo.datasource.connector;

import java.util.Arrays;
import java.util.List;

import org.wheatinitiative.vivo.datasource.connector.impl.Rcuk;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class RcukTest extends TestCase {

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
     * Test that the object is constructed properly
     */
    public void testConstructor()
    {
        List<String> queryTerms = Arrays.asList("wheat", "cheese", "grâu", "brânzã");
        Rcuk rcuk = new Rcuk(queryTerms);
        assertEquals(queryTerms, rcuk.getQueryTerms());
    }
    
}
