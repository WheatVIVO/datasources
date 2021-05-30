package org.wheatinitiative.vivo.datasource.normalizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.VivoVocabulary;
import org.wheatinitiative.vivo.datasource.connector.InsertOnlyConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.NameProcessingUtils;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDFS;

public class OrganizationNameForSameAsNormalizer extends InsertOnlyConnectorDataSource implements DataSource { 

    private static final String ABBREVIATION = VivoVocabulary.VIVO + "abbreviation";
    private static final Log log = LogFactory.getLog(OrganizationNameForSameAsNormalizer.class);

    @Override 
    public int getBatchSize() {
        return 1000;
    }

    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        return new AuthorNameIterator(this.getSparqlEndpoint(), 
                this.getConfiguration().getResultsGraphURI());
    }

    private class AuthorNameIterator implements IteratorWithSize<Model> {

        private ResultSet rs;
        private NameProcessingUtils nameUtils = new NameProcessingUtils();

        public AuthorNameIterator(SparqlEndpoint endpoint, String graphURI) {
            // only the person normalizer will clear the generic predicate
            //clearPreviousResults(LABEL_FOR_SAMEAS, endpoint, graphURI);  
            clearPreviousResults(ABBREVIATION, endpoint, graphURI);  
            
            String queryStr = "SELECT ?org (MAX(?lbl) AS ?label) WHERE { \n";
            if(graphURI != null) {
                queryStr +=   "  GRAPH <" + graphURI + "> { \n";
            }
            queryStr +=       "    ?org a <" + VivoVocabulary.FOAF + "Organization> . \n" +
                    "    FILTER NOT EXISTS { ?org <" + VivoVocabulary.OBO + "BFO_0000050> ?parent } \n" +
                    "    ?org <" + RDFS.label.getURI() + "> ?lbl . \n";
            if(graphURI != null) {
                queryStr +=   "  } \n";    
            }
            queryStr +=       "} GROUP BY ?org \n";
            log.info(queryStr);
            rs = endpoint.getResultSet(queryStr);
            log.info("Result set returned");
        }

        @Override
        public boolean hasNext() {
            return rs.hasNext();
        }

        @Override
        public Model next() {
            Model m = ModelFactory.createDefaultModel();
            QuerySolution qsoln = rs.next();
            if(!qsoln.get("org").isURIResource() || !qsoln.get("label").isLiteral()) { 
                return m;
            } else {
                Resource org = qsoln.getResource("org");
                String label = qsoln.getLiteral("label").getLexicalForm();
                String[] result = normalizeLabel(label);
                m.add(org, m.getProperty(LABEL_FOR_SAMEAS), result[0]);
                if(result[1] != null) {
                    m.add(org, m.getProperty(ABBREVIATION), result[1]);
                }
                return m;    
            }            
        }

        /**
         * Return normalized form of label, plus abbreviation if available
         * @param label the un-normalized value
         * @return array where the first string is the normalized label
         *         and the second string is an abbreviation if available, 
         *         otherwise null
         */
        private String[] normalizeLabel(String label) {
            String abbreviation = null;
            String[] tokens = label.split(" ");
            if(tokens.length == 1 && nameUtils.isAbbreviation(tokens[0])) {
                abbreviation = tokens[0];
            } else if(!isAllCaps(label) && tokens.length > 0) {
                // Handle labels like "BMGF-Bill and Melinda Gates":
                // Examine the "BMGF-Bill" and if the part before the hyphen
                // looks like an abbreviation and the second part does not,
                // store the first part as the abbreviation and remove it from
                // the normalized label
                String[] firstTokenParts = tokens[0].split("-"); 
                if (firstTokenParts.length == 2 
                        && nameUtils.isAbbreviation(firstTokenParts[0]) 
                        && !nameUtils.isAbbreviation(firstTokenParts[1])) {                    
                    abbreviation = firstTokenParts[0];
                    tokens[0] = firstTokenParts[1];
                }                 
            }
            StringBuilder out = new StringBuilder();
            for(int i = 0; i < tokens.length; i++) {
                String token = tokens[i];
                if((i + 1 == tokens.length) && token.startsWith("(")) {
                    token = token.replaceAll("\\W", "");                    
                    if(nameUtils.isAbbreviation(token)) {
                        if(abbreviation != null) {
                            abbreviation += " " + token;
                        } else {
                            abbreviation = token;
                        }
                        token = "";
                    }
                }
                if("&".equals(token)) {
                    token = "and";
                }
                token = token.replaceAll("-", " ");
                token = token.replaceAll("\\.", "");                
                token = StringUtils.stripAccents(
                        token.toLowerCase()).trim();                
                if(token.isEmpty()) {
                    continue;
                } else {
                    if(out.length() > 0) {
                        out.append(" ");
                    }
                    out.append(token);
                }                                    
            }
            String[] result = new String[2];
            result[0] = out.toString();
            result[1] = abbreviation;
            return result;
        }
        
        /**
         * Return whether a string is already all uppercase
         * @param value
         * @return false if string is null
         */
        private boolean isAllCaps(String value) {
            if(value == null) {
                return false;
            } else {
                return (value.toUpperCase().equals(value));
            }
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
