package org.wheatinitiative.vivo.datasource.dao;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * A simple interface with a method that returns the results of a 
 * SPARQL CONSTRUCT query as a Jena RDF model
 * 
 * To keep from polluting this project with unncessary VIVO API stuff.
 * 
 * @author Brian Lowe
 *
 */
public interface ModelConstructor {

    public Model construct(String query);
    
}
