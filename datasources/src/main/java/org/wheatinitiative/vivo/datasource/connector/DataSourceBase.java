package org.wheatinitiative.vivo.datasource.connector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.wheatinitiative.vivo.datasource.DataSourceConfiguration;
import org.wheatinitiative.vivo.datasource.DataSourceStatus;
import org.wheatinitiative.vivo.datasource.impl.DataSourceConfigurationImpl;
import org.wheatinitiative.vivo.datasource.impl.DataSourceStatusImpl;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class DataSourceBase {

    protected RdfUtils rdfUtils;
    protected DataSourceStatus status = new DataSourceStatusImpl();
    protected DataSourceConfiguration configuration = 
            new DataSourceConfigurationImpl();
    
    public DataSourceBase() {
        this.rdfUtils = new RdfUtils();
    }
    
    public DataSourceConfiguration getConfiguration() {
        return this.configuration;
    }
    
    public DataSourceStatus getStatus() {
        return this.status;
    }
    
    /**
     * Load a named CONSTRUCT query from a file in the SPARQL resources 
     * directory, run it against a model and add the results back to the model.
     * @param queryName name of SPARQL CONSTRUCT query to load
     * @param m model against which query should be executed
     * @param namespaceEtc base string from which URIs should be generated
     * @return model with new data added by CONSTRUCT query
     */
    protected Model construct(String queryName, Model m, String namespaceEtc) {
        String queryStr = loadQuery(queryName);
        try {
            QueryExecution qe = QueryExecutionFactory.create(queryStr, m);
            try {
                Model tmp = ModelFactory.createDefaultModel();
                qe.execConstruct(tmp);
                m.add(rdfUtils.renameBNodes(tmp, namespaceEtc, m));
            } finally {
                if(qe != null) {
                    qe.close();
                }
            }
        } catch (QueryParseException qpe) {
            throw new RuntimeException("Error parsing query " + queryName, qpe);
        }
        return m;
    }
    
    protected String loadQuery(String resourcePath) {
        InputStream inputStream = this.getClass().getResourceAsStream(
                resourcePath);
        StringBuffer fileContents = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String ln;
            while ( (ln = reader.readLine()) != null) {
                fileContents.append(ln).append('\n');
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to load " + resourcePath, e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return fileContents.toString();
    }
    
}
