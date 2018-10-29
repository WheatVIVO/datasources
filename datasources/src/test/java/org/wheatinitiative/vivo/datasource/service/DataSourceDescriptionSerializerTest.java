package org.wheatinitiative.vivo.datasource.service;

import java.util.Arrays;
import java.util.List;

import org.wheatinitiative.vivo.datasource.DataSourceConfiguration;
import org.wheatinitiative.vivo.datasource.DataSourceDescription;
import org.wheatinitiative.vivo.datasource.DataSourceStatus;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class DataSourceDescriptionSerializerTest extends TestCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public DataSourceDescriptionSerializerTest( String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite( DataSourceDescriptionSerializerTest.class );
    }

    /**
     * Test that status/configuration can be roundtripped through the
     * serialization/deserialization process, including unicode strings
     */
    public void testSerializeUnserialize()
    {
        DataSourceStatus status = new DataSourceStatus();
        status.setCompletionPercentage(25);
        status.setErrorRecords(1);
        status.setMessage("Bună ziua");
        status.setProcessedRecords(250);
        status.setRunning(false);
        status.setTotalRecords(1000);
        DataSourceConfiguration configuration = new DataSourceConfiguration();
        List<String> queryTerms = Arrays.asList(
                "wheat", "cheese", "grâu", "brânză");
        configuration.getParameterMap().put("queryTerms", queryTerms);
        DataSourceDescription description = new DataSourceDescription(
                configuration, status);
        DataSourceDescriptionSerializer serializer = 
                new DataSourceDescriptionSerializer();
        String json = serializer.serialize(description);
        DataSourceDescription description2 = serializer.unserialize(json);
        assertEquals(queryTerms, description2.getConfiguration()
                .getParameterMap().get("queryTerms"));
        assertEquals(description.getStatus().getCompletionPercentage(), 
                description2.getStatus().getCompletionPercentage());
        assertEquals(description.getStatus().getErrorRecords(), 
                description2.getStatus().getErrorRecords());
        assertEquals(description.getStatus().getMessage(), 
                description2.getStatus().getMessage());
        assertEquals(description.getStatus().getProcessedRecords(), 
                description.getStatus().getProcessedRecords());
        assertEquals(description.getStatus().getTotalRecords(), 
                description2.getStatus().getTotalRecords());
    }
    
}
