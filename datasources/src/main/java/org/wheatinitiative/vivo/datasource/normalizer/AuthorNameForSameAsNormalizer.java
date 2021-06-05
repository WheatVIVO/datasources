package org.wheatinitiative.vivo.datasource.normalizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

public class AuthorNameForSameAsNormalizer 
        extends InsertOnlyConnectorDataSource implements DataSource {

    public static final String HAS_NORMALIZED_NAMES = "https://wheatvivo.org/ontology/local/hasNN";
    private static final Log log = LogFactory.getLog(AuthorNameForSameAsNormalizer.class);
    private NameProcessingUtils nameUtils = new NameProcessingUtils();
    
    @Override 
    public int getBatchSize() {
        return 1000;
    }

    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        String graphURI = getMostRecentVersion(this.getConfiguration().getResultsGraphURI());
        this.getConfiguration().setResultsGraphURI(graphURI);
        return new AuthorNameIterator(this.getSparqlEndpoint(), graphURI);
    }

    private class AuthorNameIterator implements IteratorWithSize<Model> {

        private ResultSet rs;
        private String currentHash = null;
        int iterationCount = 0;
        private String NS;
        
        public AuthorNameIterator(SparqlEndpoint endpoint, String graphURI) {
            this.NS = getDefaultNamespace() + "NN-";
            clearOldResults(endpoint, graphURI);    
            
            String queryStr = "SELECT ?person ?familyName ?givenName ?hash WHERE { \n";
            queryStr += "{ \n";
            // Use MIN for one and MAX for other as a simple way of avoiding
            // problems with messy Name objects where values A and B are both
            // asserted as the family name and as the given name.
            queryStr += "SELECT ?person (MIN(STR(?lastName)) AS ?familyName) (MAX(STR(?firstName)) AS ?givenName) WHERE { \n";
            if(graphURI != null) {
                queryStr += "  GRAPH <" + graphURI + "> { \n";
            }
            queryStr +=       "    ?person a <" + VivoVocabulary.PERSON + "> . \n" +
                    "    ?person <" + VivoVocabulary.HAS_CONTACT + "> ?vcard . \n" +
                    "    ?vcard <" + VivoVocabulary.VCARD + "hasName> ?name . \n" +
                    "    ?name <" + VivoVocabulary.VCARD + "familyName> ?lastName . \n" +
                    "    ?name <" + VivoVocabulary.VCARD + "givenName> ?firstName . \n";
            if(graphURI != null) {
                queryStr += "  } \n";
            }            
            queryStr += "} GROUP BY ?person \n";
            queryStr += "} \n";
            queryStr += "BIND(MD5(CONCAT(?familyName, \", \", ?givenName)) AS ?hash) \n";
            queryStr += "} ORDER BY ?hash";
            log.info(queryStr);
            rs = endpoint.getResultSet(queryStr);                              
        }
        
        private void clearOldResults(SparqlEndpoint endpoint, String graphURI) {
            clearPreviousResults(LABEL_FOR_SAMEAS, endpoint, graphURI);                    
            clearPreviousResults(LABEL_FOR_SAMEAS + "1", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "2", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "3", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "4", endpoint, graphURI);
            clearPreviousResults(HAS_NORMALIZED_NAMES, endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "A1", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "A2", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "A3", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "B1", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "B2", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "B3", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "C1", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "C2", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "C3", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "RB1", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "RC1", endpoint, graphURI);
            clearPreviousResults(LABEL_FOR_SAMEAS + "RC3", endpoint, graphURI);
        }

        @Override
        public boolean hasNext() {
            return rs.hasNext();
        }

        private static final boolean SPLIT_ON_HYPHEN = true;
        
        @Override
        public Model next() {
            if(iterationCount > 0 && iterationCount % 1000 == 0) {
                log.info(iterationCount + " processed");
            }
            iterationCount++;
            Model m = ModelFactory.createDefaultModel();
            QuerySolution qsoln = rs.next();
            if(!qsoln.contains("person") 
                    || !qsoln.get("person").isURIResource() 
                    || !qsoln.get("familyName").isLiteral()
                    || !qsoln.get("givenName").isLiteral() ) {
                return m;
            } else {
                Resource person = qsoln.getResource("person");
                String familyName = qsoln.getLiteral("familyName").getLexicalForm().trim();
                String givenName = qsoln.getLiteral("givenName").getLexicalForm().trim();
                String hash = qsoln.getLiteral("hash").getLexicalForm();
                m.add(person, m.getProperty(HAS_NORMALIZED_NAMES),
                        m.getResource(NS + hash));
                if(hash.equals(currentHash)) {
                    return m;
                }
                String normalizedFamily = normalizeFamily(familyName); 
                String normalizedGiven = normalizeFamily(givenName); 
                List<String> givenTokens = tokenizeGiven(givenName, SPLIT_ON_HYPHEN);
                List<String> familyTokens = tokenizeGiven(familyName);
                addNormalForms(normalizedGiven, normalizedFamily, givenTokens, familyTokens, hash, m);
                givenTokens = tokenizeGiven(givenName, !SPLIT_ON_HYPHEN);
                addNormalForms(normalizedGiven, normalizedFamily, givenTokens, familyTokens, hash, m);
                if(familyTokens.size() > 1) {
                    String firstFamily = familyTokens.remove(0);
                    givenTokens.add(firstFamily);
                    // try treating first part of multipart surnames as 
                    // middle names, because sources will also make this mistake
                    addNormalForms(normalizeFamily(reassemble(givenTokens)), 
                            normalizeFamily(reassemble(familyTokens)), 
                            givenTokens, familyTokens, hash, m);
                }
                currentHash = hash;
                log.debug("returning " + m.size() + " triples");
                return m;
            }            
        }
        
        protected String reassemble(List<String> tokens) {
            StringBuilder builder = new StringBuilder();
            for(String token : tokens) {
                if(builder.length() > 0) {
                    builder.append(" ");
                }
                builder.append(token);
            }
            return builder.toString();
        }
        
        protected String normalizeFamily(String value) {
            StringBuilder builder = new StringBuilder();
            String lowerValue = value.toLowerCase();
            if(lowerValue.length() == value.length()) {
                value = lowerValue;
            }
            String strippedValue = StringUtils.stripAccents(value);
            if(strippedValue.length() == value.length()) {
                value = strippedValue;
            }
            value = value.replaceAll("\\W+", "");
            String[] tokens = value.split(" ");
            for(int i = 0; i < tokens.length; i++) {
                String token = tokens[i];
                if(token.length() == 0) {
                    continue;
                }
                if(builder.length() > 0) {
                    builder.append(" ");
                }
                builder.append(token);
            }
            return builder.toString();
        }
        
        protected void addNormalForms(String normalizedGiven, String normalizedFamily, 
                List<String> givenTokens, List<String> familyTokens, String hash, Model m) {
            int[] piecesCounts = countInitialsAndNonInitials(givenTokens);
            int initials = piecesCounts[0];
            int words = piecesCounts[1];
            log.debug("Initials: " + initials + " Words: " + words);
            if(initials < 1 && words < 1) {
                return;
            }
            tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                    + "A1"), normalForm(1, 0, givenTokens, initials, words, normalizedFamily));
            tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                    + "A2"), normalForm(2, 0, givenTokens, initials, words, normalizedFamily));
            tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                    + "A3"), normalForm(3, 0, givenTokens, initials, words, normalizedFamily));
            tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                    + "B1"), normalForm(0, 1, givenTokens, initials, words, normalizedFamily));
            tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                    + "B2"), normalForm(1, 1, givenTokens, initials, words, normalizedFamily));
            tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                    + "B3"), normalForm(2, 1, givenTokens, initials, words, normalizedFamily));
            tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                    + "C1"), normalForm(0, 2, givenTokens, initials, words, normalizedFamily));
            tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                    + "C2"), normalForm(1, 2, givenTokens, initials, words, normalizedFamily));
            tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                    + "C3"), normalForm(0, 3, givenTokens, initials, words, normalizedFamily));
            if(initials == 0) {
                tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                        + "RB1"), normalForm(0, 1, familyTokens, initials, words, normalizedGiven));
                tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                        + "RC1"), normalForm(0, 2, familyTokens, initials, words, normalizedGiven));
                tryAdd(m, m.getResource(NS + hash), m.getProperty(LABEL_FOR_SAMEAS
                        + "RC3"), normalForm(0, 3, familyTokens, initials, words, normalizedGiven));
            }
        }
        
        private void tryAdd(Model m, Resource resource, Property property, String string) {
            if(m != null && resource != null && property != null && string != null) {
                m.add(resource, property, string);
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
        // not used with this type of connector
        return null;
    }
    
    /**
     * Return the lowercase first character of a string 
     * @param token
     * @return first character in lowercase form
     */
    private String grabInitial(String token) {
        String charStr = Character.toString(token.charAt(0));
        String lowerCharStr = charStr.toLowerCase();
        return (lowerCharStr.length() == charStr.length()) ? lowerCharStr : charStr; 
    }
    
    private StringBuilder appendSpace(StringBuilder builder) {
        if(builder.length() > 0) {
            builder.append(" ");
        }
        return builder;
    }    
    
    private int[] countInitialsAndNonInitials(List<String> givenTokens) {
        int[] counts = new int[2];
        for(String token : givenTokens) {            
            if(nameUtils.isInitials(token)) {
                counts[0]++;
            } else if(!token.isEmpty()) {
                counts[1]++;
            }
        }
        return counts;
    }
    
    private boolean isHyphenatedInitials(String token) {
        return (token.length() == 3 
                && Character.isLetter(token.codePointAt(0))
                && Character.isLetter(token.codePointAt(2))
                && '-' == token.codePointAt(1) );
    }
    
    private List<String> tokenizeGiven(String givenName) {
        return tokenizeGiven(givenName, false);
    }
    
    private List<String> tokenizeGiven(String givenName, boolean splitOnHyphen) {
        List<String> tokenList = new ArrayList<String>();
        String strippedGivenName = StringUtils.stripAccents(givenName);
        if(strippedGivenName.length() == givenName.length()) {
            givenName = strippedGivenName;
        }
        givenName = givenName.replaceAll("\\.-", "-");
        givenName = givenName.replaceAll("\\.", " ");
        if(splitOnHyphen) {
            givenName = givenName.replaceAll("\\-", " ");
        }
        List<String> tokens = Arrays.asList(givenName.split(" "));
        for(String token : tokens) {                
            if(isHyphenatedInitials(token)) {
                tokenList.add(token);
            } else {
                token = token.trim();
                token = token.replaceAll("\\W", "-");
                if(token.length() == 0) {
                    continue;
                }
                if(!nameUtils.isInitials(token)) {
                    tokenList.add(token);
                } else {
                    for(int i = 0; i < token.length(); i++) {
                        if(Character.isLetter(token.codePointAt(i))) {
                            tokenList.add(String.valueOf(token.charAt(i)));
                        }
                    }    
                }                    
            }
        }
        return tokenList;
    }

    private String substituteNickname(String token) {
        // TODO placeholder
        return null;
    }
    
    public String normalForm(int requiredInitials, int requiredWords, 
            List<String> givenTokens, int initials, int words, String normalizedFamily) {
        if(requiredInitials + requiredWords == 0) {
            return null;
        }
        if(givenTokens.size() < requiredInitials + requiredWords) {
            return null;
        }
        if(words < requiredWords) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        int initialsToInclude = (requiredInitials + requiredWords - words);
        for(String token : givenTokens) {
            if(nameUtils.isInitials(token)) {
                if(requiredInitials > 0 && initialsToInclude > 0) {
                    appendSpace(builder).append(grabInitial(token));
                    initialsToInclude-- ;
                    requiredInitials-- ;
                } else if((requiredWords > 0 || words > 0 ) && builder.length() > 0) {
                    // J. S. Bach should be j s bach even if only one initial is requested
                    appendSpace(builder).append(grabInitial(token));
                }
            } else if(!token.isEmpty()) {
                if(requiredWords == 0 && requiredInitials > 0) {
                    appendSpace(builder).append(grabInitial(token));
                    requiredInitials-- ;
                    words--;
                } else if (requiredWords > 0) {
                    appendSpace(builder).append(normalizeGivenToken(token));
                    requiredWords--;
                    words--;
                }
            }
        }        
        if(builder.length() > 0 && requiredInitials == 0 && requiredWords == 0) {
            return normalizedFamily + ", " + builder.toString();
        } else {
            return null;
        }
    }
    
    private String normalizeGivenToken(String token) {
        String lowerToken = token.toLowerCase();
        if(lowerToken.length() == token.length()) {
            token = lowerToken;
        }
        String norm = token.replaceAll("\\W", "-");
        String normNoNick = substituteNickname(norm);
        return (normNoNick != null) ? normNoNick : norm;        
    }

}
