package org.wheatinitiative.vivo.datasource.connector;

import java.io.IOException;

import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.csv.CsvToRdf;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.vocabulary.RDF;

public abstract class CsvDataSource extends ConnectorDataSource {
    
    /**
     * to be overridden by subclasses
     * @return Csv2Rdf object that converts to RDF 
     * CSV files of the expected format
     */
    protected abstract CsvToRdf getCsvConverter();
    
    /**
     * to be overridden by subclasses
     * @return the String to which a unique ID part will be concatenated
     * when generating URIs for ABox individuals
     */
    protected abstract String getABoxNamespaceAndPrefix();
    
    protected String getServiceURI() {
        return this.getConfiguration().getServiceURI();
    }
    
    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        HttpUtils httpUtils = new HttpUtils();
        try {
            String csvString = httpUtils.getHttpResponse(getServiceURI());
            CsvToRdf csvParser = getCsvConverter();
            return new CSVSourceModelIterator(
                    csvParser.getModelIterator(csvString));
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
  
    private class CSVSourceModelIterator implements IteratorWithSize<Model> {

        private IteratorWithSize<Model> inner;
        Model uriDedupModel = ModelFactory.createDefaultModel();
        
        public CSVSourceModelIterator(IteratorWithSize<Model> inner) {
            this.inner = inner;
        }
        
        public boolean hasNext() {
            return inner.hasNext();
        }

        public Model next() {
            Model model = inner.next();
            model = rdfUtils.renameBNodes(model, getABoxNamespaceAndPrefix(),
                    uriDedupModel);
            registerURIsForDeduplication(model);
            return model;
        }
        
        private void registerURIsForDeduplication(Model model) {
            uriDedupModel.add(model.listStatements(null, RDF.type, 
                    (RDFNode) null));
        }

        public Integer size() {
            return inner.size();
        }
        
    }
    
}


