package org.wheatinitiative.vivo.datasource.connector;

import java.io.InputStream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import junit.framework.TestCase;

public abstract class ConnectorTestCase extends TestCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ConnectorTestCase( String testName ) {
        super( testName );
    }
    
    /**
     * Load a model from an N3 serialization at the specified resource path
     * @param resourcePath
     * @return model containing statements read
     */
    protected Model loadModelFromResource(String resourcePath) {
        return loadModelFromResource(resourcePath, "N3");
    }
    
    /**
     * Load a model from an RDF serialization of the specified syntax at the
     * specified resource path
     * @param resourcePath
     * @param syntax
     * @return model containing statements read
     */
    protected Model loadModelFromResource(String resourcePath, String syntax) {
        Model model = ModelFactory.createDefaultModel();
        InputStream is = this.getClass().getResourceAsStream(resourcePath);
        model.read(is, null, syntax);
        return model;
    }
    
}
