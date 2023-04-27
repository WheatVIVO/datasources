package org.wheatinitiative.vivo.datasource.normalizer;

import org.apache.commons.lang3.StringUtils;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.VivoVocabulary;
import org.wheatinitiative.vivo.datasource.connector.InsertOnlyConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDFS;

public class LiteratureNameForSameAsNormalizer extends InsertOnlyConnectorDataSource implements DataSource {
    
    @Override 
    public int getBatchSize() {
        return 1000;
    }
    
    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        return new LiteratureNameIterator(this.getSparqlEndpoint(), 
                this.getConfiguration().getResultsGraphURI());
    }
    
    private class LiteratureNameIterator implements IteratorWithSize<Model> {
        
        private ResultSet rs;
        
        public LiteratureNameIterator(SparqlEndpoint endpoint, String graphURI) {
            // Only the person normalizer will clear the results
            //clearPreviousResults(LABEL_FOR_SAMEAS, endpoint, graphURI);  
            
            String queryStr = "SELECT ?journal ?label WHERE { \n";
            if(graphURI != null) {
                queryStr +=   "  GRAPH <" + graphURI + "> { \n";
            }
            queryStr +=       "    { ?journal a <" + VivoVocabulary.BIBO 
                    + "Journal> } UNION { ?journal a <" 
                    + VivoVocabulary.SKOS + "Concept> } UNION { ?journal <" 
                    + VivoVocabulary.VIVO + "dateTimeValue> ?dtv } \n" +
                              "    ?journal <" + RDFS.label.getURI() + "> ?label . \n";
            if(graphURI != null) {
                queryStr +=   "  } \n";    
            }
            queryStr +=       "} \n";
            rs = endpoint.getResultSet(queryStr);                              
        }

        @Override
        public boolean hasNext() {
            return rs.hasNext();
        }

        @Override
        public Model next() {
            Model m = ModelFactory.createDefaultModel();
            QuerySolution qsoln = rs.next();
            if(!qsoln.get("journal").isURIResource() || !qsoln.get("label").isLiteral()) { 
                return m;
            } else {
                Resource journal = qsoln.getResource("journal");
                String label = qsoln.getLiteral("label").getLexicalForm();
                m.add(journal, m.getProperty(LABEL_FOR_SAMEAS), normalizeLabel(label));
                return m;    
            }            
        }
        
        private String normalizeLabel(String label) {
            label = label.replaceAll("-", " ");
            String[] tokens = label.split(" ");
            StringBuilder out = new StringBuilder();
            for(int i = 0; i < tokens.length; i++) {
                String token = tokens[i];
                if(token.length() == 0) {
                    continue;
                } else if(token.startsWith("[") && token.endsWith("]")) {
                    continue;
                } else {
                    token = StringUtils.stripAccents(
                            tokens[i].toLowerCase()).trim();
                    if("&".equals(token)) {
                        token = "and";
                    }
                    if(token.isEmpty()) {
                        continue;
                    } else {
                        if(out.length() > 0) {
                            out.append(" ");
                        }
                        out.append(token);
                    }
                }                
            }
            return out.toString();
        }

        @Override
        public Integer size() {            
            return null;
        }

    }

    @Override
    protected Model filter(Model model) {
        return model;
    }

    @Override
    protected Model mapToVIVO(Model model) {
        return model;
    }

    @Override
    protected String getPrefixName() {
        // TODO Auto-generated method stub
        return null;
    }
}
