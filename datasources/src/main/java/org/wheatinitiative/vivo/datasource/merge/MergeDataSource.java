package org.wheatinitiative.vivo.datasource.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.DataSourceBase;
import org.wheatinitiative.vivo.datasource.connector.InsertOnlyConnectorDataSource;
import org.wheatinitiative.vivo.datasource.normalizer.AuthorNameForSameAsNormalizer;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class MergeDataSource extends DataSourceBase implements DataSource {

    private static final Log log = LogFactory.getLog(MergeDataSource.class);
    private static final String SPARQL_RESOURCE_DIR = "/merge/sparql/";
    // default size of window of adjacent values for fuzzy string comparisons
    private static final int DEFAULT_WINDOW_SIZE = 100;     
    private static final String VIVO = "http://vivoweb.org/ontology/core#";
    private static final String RELATIONSHIP = VIVO + "Relationship";
    private static final String ROLE = "http://purl.obolibrary.org/obo/BFO_0000023";
    private static final String PROCESS = "http://purl.obolibrary.org/obo/BFO_0000015";
    private static final String REALIZED_IN = "http://purl.obolibrary.org/obo/BFO_0000054";
    private static final String INHERES_IN = "http://purl.obolibrary.org/obo/RO_0000052";    
    private static final String APPLICATION_CONTEXT_NS = "http://vitro.mannlib.cornell.edu/ns/vitro/ApplicationConfiguration#";
    private static final String CONFIG_CONTEXT_FOR = APPLICATION_CONTEXT_NS + "configContextFor"; 
    private static final String QUALIFIED_BY = APPLICATION_CONTEXT_NS + "qualifiedBy";            
    private static final String ADMIN_APP_TBOX = 
            "http://vivo.wheatinitiative.org/ontology/adminapp/";
    private static final String MERGERULE = ADMIN_APP_TBOX + "MergeRule";
    private static final String DISABLED = ADMIN_APP_TBOX + "disabled";
    private static final String MERGERULEATOM = ADMIN_APP_TBOX + "MergeRuleAtom";
    private static final String HASMERGERULE = ADMIN_APP_TBOX + "hasMergeRule";
    private static final String MERGERULECLASS = ADMIN_APP_TBOX + "mergeRuleClass";
    private static final String PRIORITY = ADMIN_APP_TBOX + "priority";
    private static final String HASATOM = ADMIN_APP_TBOX + "hasAtom";
    private static final String MATCHDEGREE = ADMIN_APP_TBOX + "matchDegree";
    private static final String MERGEATOMDATAPROPERTY = ADMIN_APP_TBOX + "mergeAtomDataProperty";
    private static final String MERGEATOMOBJECTPROPERTY = ADMIN_APP_TBOX + "mergeAtomObjectProperty";
    private static final String HASCONTACTINFO = "http://purl.obolibrary.org/obo/ARG_2000028";
    private static final String VCARD = "http://www.w3.org/2006/vcard/ns#";
    private static final String FOAF_PERSON = "http://xmlns.com/foaf/0.1/Person";
    private static final String FOAF_ORGANIZATION = "http://xmlns.com/foaf/0.1/Organization";
    private static final String BIBO_COLLECTION = "http://purl.org/ontology/bibo/Collection";
    private static final String BIBO_DOCUMENT = "http://purl.org/ontology/bibo/Document";
    private static final String BASIC_SAMEAS_GRAPH = "http://vitro.mannlib.cornell.edu/a/graph/basicSameAs";
    private static final String TRANSITIVE_SAMEAS_GRAPH = "http://vitro.mannlib.cornell.edu/a/graph/transitiveSameAs";
    private static final String NORM_PROP_BASE = InsertOnlyConnectorDataSource.LABEL_FOR_SAMEAS;
        
    private Model result = ModelFactory.createDefaultModel();
    protected LevenshteinDistance ld = LevenshteinDistance.getDefaultInstance();
    
    @Override
    protected void runIngest() {        
        String dataSourceURI = this.getConfiguration().getURI();
        if(this.getSparqlEndpoint() == null) {
            throw new RuntimeException("SPARQL endpoint must be configured "
                    + " for the merge data source");
        }
        log.info("Starting merge " + dataSourceURI);
        Model rulesModel = retrieveMergeRulesFromEndpoint(this.getSparqlEndpoint());
        Model differentFromModel = getDifferentFromModel(this.getSparqlEndpoint());
        Model fauxPropertyContextModel = ModelFactory.createDefaultModel();
        String serviceURI = this.getConfiguration().getServiceURI(); 
        if(serviceURI == null) {
            throw new RuntimeException("Service URI must be set to the base" 
                    + "URL of this adminapp installation, e.g. "
                    + "http://localhost:8080/wheatvivo-adminapp/");
        }
        if(!serviceURI.endsWith("/")) {
            serviceURI += "/";
        }
        String fauxPropertyModelURI = serviceURI + "fauxPropertyContexts";
        log.info("Retrieving faux property contexts from " + fauxPropertyModelURI);
        fauxPropertyContextModel.read(fauxPropertyModelURI);
        log.info(fauxPropertyContextModel.size() + " faux property context statements.");
        int windowSize = getWindowSize(this.getSparqlEndpoint());
        this.getStatus().setMessage("adding basic sameAs assertions");
        addBasicSameAsAssertions(this.getSparqlEndpoint());
        log.info("Clearing previous merge state");
        this.getStatus().setMessage("clearing previous merge results");
        List<MergeRule> mergeRules = new ArrayList<MergeRule>();
        for(String mergeRuleURI : getMergeRuleURIs(dataSourceURI)) {
            getSparqlEndpoint().clearGraph(mergeRuleURI); 
            mergeRules.add(getMergeRule(mergeRuleURI, rulesModel));
        }
        SparqlEndpoint endpoint = getSparqlEndpoint();
        clearTransitiveSameAsAssertions(endpoint);        
        this.getStatus().setMessage("running merge rules");
        Collections.sort(mergeRules, new AffectedClassRuleComparator(getSparqlEndpoint()));        
        Map<String, Long> statistics = new HashMap<String, Long>();
        for(int i = 0; i < 2; i++) {
            for(MergeRule rule : mergeRules) {
                String mergeRuleURI = rule.getURI();
                // TODO flush to endpoint and repeat rules until quiescent?
                log.info("Processing rule " + mergeRuleURI);                         
                Model ruleResult = getSameAs(rule, fauxPropertyContextModel, 
                        this.getSparqlEndpoint(), windowSize);
                if(isSuspicious(ruleResult)) {
                    log.warn(mergeRuleURI + " produced a suspiciously large number (" + 
                            ruleResult.size() + ") of triples." );
                }
                filterObviousResults(ruleResult);
                filterKnownDifferentFrom(ruleResult, differentFromModel);
                //result.add(ruleResult);
                Long stat = statistics.get(mergeRuleURI);
                if(stat == null) {
                    statistics.put(mergeRuleURI, ruleResult.size());    
                } else {
                    statistics.put(mergeRuleURI, stat + ruleResult.size());
                }
                
                log.info("Rule results size: " + ruleResult.size());            
                getSparqlEndpoint().writeModel(ruleResult, mergeRuleURI); 
            }
            addTransitiveSameAsAssertions(endpoint);
        }
        String resultsGraphURI = getConfiguration().getResultsGraphURI();
        getSparqlEndpoint().clearGraph(resultsGraphURI); 
        log.info("Merging relationships");
        this.getStatus().setMessage("merging relationships");
        Model tmp = getRelationshipSameAs();
        log.info(tmp.size() + " sameAs from merged relationships");
        getSparqlEndpoint().writeModel(tmp, resultsGraphURI);
        try {
            log.info("Merging roles");
            this.getStatus().setMessage("merging roles");
            tmp = getRoleSameAs();
            log.info(tmp.size() + " sameAs from merged roles");
            getSparqlEndpoint().writeModel(tmp, resultsGraphURI);
        } catch (Exception e) {
            log.error(e, e);
        }
        this.getStatus().setMessage("merging vCards");
        tmp = getVcardSameAs(endpoint);
        log.info(tmp.size() + " sameAs from merged vCards");
        getSparqlEndpoint().writeModel(tmp, resultsGraphURI);
        tmp = getVcardPartsSameAs(endpoint);
        getSparqlEndpoint().writeModel(tmp, resultsGraphURI);
        addTransitiveSameAsAssertions(endpoint);
        log.info("======== Final Results ========");
        for(String ruleURI : statistics.keySet()) {            
            log.info("Rule " + ruleURI + " added " + statistics.get(ruleURI));
        }
    }
    
    /**
     * Materialize inferences of type sameAs(x,x) for query support
     */
    protected void addBasicSameAsAssertions(SparqlEndpoint endpoint) {
        String queryStr = "CONSTRUCT { ?x <" + OWL.sameAs.getURI() + "> ?x } WHERE { \n" +
                "    ?x a ?thing \n" +
                "    FILTER NOT EXISTS { ?x <" + OWL.sameAs.getURI() + "> ?x } \n" +
                "} \n";
        Model m = endpoint.construct(queryStr);
        log.info("Writing " + m.size() + " triples to " + BASIC_SAMEAS_GRAPH);
        String delQueryStr = "CONSTRUCT { ?x <" + OWL.sameAs.getURI() + "> ?x } WHERE { \n" +
                "    GRAPH <" + BASIC_SAMEAS_GRAPH + "> { ?x <" + OWL.sameAs.getURI() + "> ?x } \n" +
                "    FILTER NOT EXISTS { ?x a ?thing } \n" +
                "} \n";
        Model toDelete = endpoint.construct(delQueryStr);
        log.info("Deleting " + toDelete.size() + " triples from " + BASIC_SAMEAS_GRAPH);
        endpoint.deleteModel(toDelete, BASIC_SAMEAS_GRAPH);        
    }
    
    protected void clearTransitiveSameAsAssertions(SparqlEndpoint endpoint) {
        log.info("Clearing " + TRANSITIVE_SAMEAS_GRAPH);
        endpoint.clear(TRANSITIVE_SAMEAS_GRAPH);
    }
    
    /**
     * Materialize inferences of type sameAs(x,x) for query support
     */
    protected void addTransitiveSameAsAssertions(SparqlEndpoint endpoint) {
        int maxIterations = 3;
        long inferenceCount = 1;
        while(inferenceCount > 0 && maxIterations > 0) {
            maxIterations--;
            String queryStr = "CONSTRUCT { ?x <" + OWL.sameAs.getURI() + "> ?z } WHERE { \n" +
                    "    ?x <" + OWL.sameAs.getURI() + "> ?y . \n" +
                    "    ?y <" + OWL.sameAs.getURI() + "> ?z . \n" +
                    "    FILTER NOT EXISTS { ?x <" + OWL.sameAs.getURI() + "> ?z } \n" +
                    "} \n";
            Model m = endpoint.construct(queryStr);
            log.info("Writing " + m.size() + " triples to " + TRANSITIVE_SAMEAS_GRAPH);
            inferenceCount = m.size();
            endpoint.writeModel(m, TRANSITIVE_SAMEAS_GRAPH);
        }
    }
    
    /**
     * Check if a result model is likely to contain an unwanted Cartesian product
     * @param m 
     * @return true if the number of triples in the model is greater than
     * one half the square of the number of distinct URIs
     */
    protected boolean isSuspicious(Model m) {
        if(m.size() < 128) {
            return false;
        }
        String distinctURIs = "SELECT (COUNT(DISTINCT ?x) AS ?count) WHERE { \n" +
                "    { ?x ?p ?o } UNION { ?s ?p ?x } \n" +
                "} \n";
        QueryExecution qe = QueryExecutionFactory.create(distinctURIs, m);
        try {
            ResultSet rs = qe.execSelect();
            while(rs.hasNext()) {
                QuerySolution qsoln = rs.next();
                RDFNode node = qsoln.get("count");
                if(node.isLiteral()) {
                    int distinctURICount = Integer.parseInt(
                            node.asLiteral().getLexicalForm(), 10);
                    boolean suspicious = m.size() >= ((distinctURICount * distinctURICount) / 2);
                    log.info("Distinct URIs: " + distinctURICount + "; result size: " + m.size());
                    log.info(suspicious ? "suspicious!" : "not suspicious");
                    return suspicious;
                }
            }
            return false;
        } finally {
            qe.close();
        }
    }
    
    /**
     * Remove each statement of the type owl:sameAs(x,y) from model m where
     * differentFromModel contains a statement owl:differentFrom(x,y).
     * @param m the model containing sameAs statements to be filtered
     * @param differentFromModel the model containing differentFrom statements
     */
    private void filterKnownDifferentFrom(Model m, Model differentFromModel) {
        if(differentFromModel == null || differentFromModel.isEmpty()) {
            return;
        }
        Model delete = ModelFactory.createDefaultModel();
        StmtIterator sit = m.listStatements(null, OWL.sameAs, (Resource) null);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            if(differentFromModel.contains(
                    stmt.getSubject(), OWL.differentFrom, stmt.getObject())) {
                delete.add(stmt);
            }
        }
        m.remove(delete);
    }
    
    private void filterObviousResults(Model m) {
        Model delete = ModelFactory.createDefaultModel();
        StmtIterator sit = m.listStatements();
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            if(stmt.getObject().isURIResource() 
                    && stmt.getObject().asResource().getURI().equals(stmt.getSubject().getURI())) {
                delete.add(stmt);
            }
        }
        m.remove(delete);
    }
    
    private Model getSameAs(MergeRule rule, Model fauxPropertyContextModel, 
            SparqlEndpoint sparqlEndpoint, int windowSize) {
        Model sameAsModel = null;
        boolean firstAtom = true;
        for (MergeRuleAtom atom : rule.getAtoms()) {
            if(!firstAtom && sameAsModel.isEmpty()) {
                return sameAsModel;
            }
            firstAtom = false;
            log.debug("Processing atom " + atom.getMergeDataPropertyURI() + " ; " 
                    + atom.getMergeObjectPropertyURI() + " ; " 
                    + atom.getMatchDegree());
            if(AuthorNameForSameAsNormalizer.HAS_NORMALIZED_NAMES.equals(
                    atom.getMergeObjectPropertyURI())) {
                sameAsModel = join(sameAsModel, executePersonNameMatch(sparqlEndpoint)); 
            } else if(atom.getMatchDegree() < 100) {
                sameAsModel = join(sameAsModel, getFuzzySameAs(
                        rule, atom, fauxPropertyContextModel, windowSize));
                sameAsModel = supplementFuzzySameAs(sameAsModel, sparqlEndpoint);
            } else {
                String queryStr = null;
                if (atom.getMergeObjectPropertyURI() != null) {
                    queryStr = getObjectPropertySameAs(
                            rule, atom, fauxPropertyContextModel, sparqlEndpoint);                
                } else if(atom.getMergeDataPropertyURI() != null) {
                    log.info("data property: " + atom.getMergeDataPropertyURI());
                    if(atom.getMergeDataPropertyURI().startsWith(VCARD) 
                            && atom.getMergeDataPropertyURI().endsWith("Name")) {
                        queryStr = getVcardNameSameAs(rule, atom);                    
                    } else {
                        queryStr = getDataPropertySameAs(rule, atom);    
                    }
                } 
                if(queryStr == null) {
                    log.info("Incomplete atoms; no query string generated."); 
                    continue;
                } else {
                    queryStr = "CONSTRUCT { \n " +
                            "    ?x <" + OWL.sameAs.getURI() + "> ?y . \n" +
                            "    ?y <" + OWL.sameAs.getURI() + "> ?x  \n" +
                            "} WHERE { \n"
//                            + "  FILTER NOT EXISTS { ?x <" + OWL.sameAs + "> ?y } \n" 
                            + queryStr +
//                            "    ?x <" + OWL.sameAs.getURI() + "> ?x1 . \n" +
//                            "    ?y <" + OWL.sameAs.getURI() + "> ?y1 . \n" +
                            "    FILTER NOT EXISTS { ?x <" + OWL.sameAs.getURI() + "> ?y } \n" +
                            "    FILTER NOT EXISTS { ?y <" + OWL.sameAs.getURI() + "> ?x } \n" +
                            "    FILTER NOT EXISTS { ?x <" + OWL.differentFrom.getURI() + "> ?y } \n" +
                            "    FILTER NOT EXISTS { ?y <" + OWL.differentFrom.getURI() + "> ?x } \n" +
                            "    #VALUES \n" +
                            "} \n";                   
                    log.info("Generated sameAs query: \n" + queryStr);                    
                    sameAsModel = join(sameAsModel, constructWithValues(queryStr, sameAsModel));
                }
            }
        }         
        return sameAsModel;
    }
    
    private Model constructWithValues(String queryStr, Model valuesModel) {
        if(valuesModel == null || valuesModel.isEmpty()) {
            return sparqlEndpoint.construct(queryStr);
        } else {
            Model results = ModelFactory.createDefaultModel();
            List<Statement> valuesForQuery = new ArrayList<Statement>();
            boolean firstIteration = true;
            int count = 0;
            StmtIterator existIt = valuesModel.listStatements();
            while(existIt.hasNext()) {
                Statement existStmt = existIt.next();
                valuesForQuery.add(existStmt);
                count++;
                if(count == 250 || !existIt.hasNext()) {
                    StringBuilder values = new StringBuilder();
                    values.append("VALUES (?x ?y) { \n");
                    for(Statement exist : valuesForQuery) {
                        if(exist.getSubject().isURIResource() 
                                && exist.getObject().isURIResource()) {
                            values.append("  (<").append(exist.getSubject().getURI()).append("> ");
                            values.append("<").append(exist.getObject().asResource().getURI()).append(">) \n");
                        }
                    }
                    values.append("} \n");
                    queryStr = queryStr.replace("#VALUES", values.toString());
                    if(firstIteration) {
                        log.info(queryStr);
                        firstIteration = false;
                    }
                    results.add(sparqlEndpoint.construct(queryStr));
                    valuesForQuery.clear();
                    count = 0;
                }         
            }
            return results;
        }
    }
    
    private Model join(Model sameAsModel, Model atomModel) {
        if(sameAsModel == null) {
            return atomModel;
        } else {
            Model intersection = sameAsModel.intersection(atomModel);
            log.info("Intersection=" + intersection.size() + "; previous=" 
                    + sameAsModel.size() + "; atom=" + atomModel.size());
            return intersection;
        }
    }
    
    private Model executePersonNameMatch( 
            SparqlEndpoint endpoint) {
        return executePersonNameMatchQuery("personSameName.rq", endpoint);                               
    }
    
    private Model executePersonNameMatchQuery(String queryFile, 
            SparqlEndpoint endpoint) {
        Model results = ModelFactory.createDefaultModel();
        List<String> xNormP = Arrays.asList("C3", "RC3", "C2", "C1", "RC1", "B3", "B2", "B1", "RB1", "A3", "A2", "A1");
        List<String> yNormP = Arrays.asList("C3",  "C3", "C2", "C1",  "C1", "B3", "B2", "B1",  "B1", "A3", "A2", "A1");
        List<String> guardP = Arrays.asList("XX",  "XX", "XX", "XX",  "XX", "C1", "C1", "C1",  "C1", "B1", "B1", "B1");
        List<String> guardPx = Arrays.asList("XX",  "XX", "C3", "C2",  "XX", "XX", "B3", "B2",  "XX", "XX", "A3", "A2");
        for(int i = 0; i < xNormP.size(); i++) {
            Map<String, String> uriBindings = new HashMap<String, String>();
            uriBindings.put("xNormP", NORM_PROP_BASE + xNormP.get(i));
            uriBindings.put("yNormP", NORM_PROP_BASE + yNormP.get(i));
            uriBindings.put("guardP", NORM_PROP_BASE + guardP.get(i));
            uriBindings.put("guardPx", NORM_PROP_BASE + guardPx.get(i));
            ParameterizedSparqlString queryStr = new ParameterizedSparqlString(
                    this.loadQuery(SPARQL_RESOURCE_DIR + queryFile));
            for(String key : uriBindings.keySet()) {
                queryStr.setIri(key, uriBindings.get(key));
            }
            log.info(queryStr.toString());
            Model m = endpoint.construct(queryStr.toString());
            StmtIterator mit = m.listStatements();
            while(mit.hasNext()) {
                Statement stmt = mit.next();
                if(!results.contains(stmt.getSubject(), null, (RDFNode) null)) {
                    results.add(stmt);
                }
            }
        }
        return results;
    }
    
    /**
     * Add additional sameAs matches (based on existing sameAs statements)
     * as if we had a reasoned model
     */
    protected Model supplementFuzzySameAs(Model m, SparqlEndpoint endpoint) {
        String inverse = "CONSTRUCT { ?y <" + OWL.sameAs.getURI() + "> ?x }" +
                " WHERE { ?x <" + OWL.sameAs.getURI() + "> ?y }";
        QueryExecution qe = QueryExecutionFactory.create(inverse, m);
        try {
            m.add(qe.execConstruct());
        } finally {
            qe.close();
        }
        Model supplement = ModelFactory.createDefaultModel();
        ResIterator rit = m.listSubjects();
        while(rit.hasNext()) {
            Resource r = rit.next();
            supplement.add(getSupplement(r, m, endpoint));
        }
        m.add(supplement);
        return m;
    }
    
    private Model getSupplement(Resource r, Model m, SparqlEndpoint endpoint) {
        Model supplement = ModelFactory.createDefaultModel();
        List<Resource> peers = new ArrayList<Resource>();
        ResultSet resultSet = endpoint.getResultSet(
                "SELECT ?y WHERE { <" + r.getURI() + ">" + 
                " <" + OWL.sameAs.getURI() + "> ?y }");
        while(resultSet.hasNext()) {
            QuerySolution qsoln = resultSet.next();
            RDFNode node = qsoln.get("y");
            if(!node.isURIResource()) {
                continue;
            }
            peers.add(node.asResource());
        }
        StmtIterator sit = m.listStatements(r, null, (RDFNode) null);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            for(Resource peer : peers) {
                supplement.add(peer, stmt.getPredicate(), stmt.getObject());
            }
        }
        return supplement;
    }
          
    private String getVcardNameSameAs(MergeRule rule, MergeRuleAtom atom) {
        return  "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?x <" + HASCONTACTINFO + "> ?vcard . \n" +
                "    ?vcard <" + VCARD + "hasName> ?name . \n" +
                "    ?name <" + atom.getMergeDataPropertyURI() + "> ?value . \n" +
                "    ?y <" + HASCONTACTINFO + "> ?vcard . \n" +
                "    ?vcard <" + VCARD + "hasName> ?name . \n" +
                "    ?name <" + atom.getMergeDataPropertyURI() + "> ?value . \n" +
                "    ?y a <" + rule.getMergeClassURI()  + "> . \n";
    }
    
    private String getSingleEndedVcardNameQuery(MergeRule rule, MergeRuleAtom atom) {
        String queryStr = "SELECT ?x ?value WHERE { \n" +
                "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?x <" + HASCONTACTINFO + "> ?vcard . \n" +
                "    ?vcard <" + VCARD + "hasName> ?name . \n";
        if((VCARD + "givenName").equals(atom.getMergeDataPropertyURI())) {
            queryStr +=        
                    "    ?name <" + VCARD + "familyName> ?familyNameValue . \n" +
                    "    ?name <" + atom.getMergeDataPropertyURI() + "> ?value . \n" +
                    "} ORDER BY ?familyNameValue ?value \n" ;
        } else {
            queryStr +=        
                "    ?name <" + atom.getMergeDataPropertyURI() + "> ?value . \n" +
                "} ORDER BY ?familyNameValue ?value \n" ;
        }
        return queryStr;
    }
    
    private String getDataPropertySameAs(MergeRule rule, MergeRuleAtom atom) {
        return  "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?x <" + atom.getMergeDataPropertyURI() + "> ?value . \n" +
                "    ?y <" + atom.getMergeDataPropertyURI() + "> ?value . \n" +
                "    ?y a <" + rule.getMergeClassURI()  + "> . \n";
    }
        
    private String getObjectPropertySameAs(MergeRule rule, MergeRuleAtom atom, 
            Model fauxPropertyContextModel, SparqlEndpoint sparqlEndpoint) {
        if(isFauxPropertyContext(atom.getMergeObjectPropertyURI(), 
                fauxPropertyContextModel)) {
            FauxPropertyContext ctx = getFauxPropertyContext(
                    atom.getMergeObjectPropertyURI(), fauxPropertyContextModel);
            if(ctx == null) {
                return null;
            } else {
                return getFauxPropertyPattern(rule, atom, ctx, sparqlEndpoint);    
            }                    
        } else {
            return  "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                    "    ?x <" + atom.getMergeObjectPropertyURI() + "> ?value . \n" +
                    "    ?value <" + OWL.sameAs.getURI() + "> ?value1 . \n" +
                    "    ?y <" + atom.getMergeObjectPropertyURI() + "> ?value1 . \n" +
                    "    ?y a <" + rule.getMergeClassURI()  + "> . \n" ;
        }
    }
    
    protected boolean isSubclass(String classURI, String superclassURI, SparqlEndpoint sparqlEndpoint) {
        if(classURI == null || superclassURI == null) {
            return false;
        }
        if(superclassURI.equals(classURI)) {
            return true;
        }
        Model ask =  sparqlEndpoint.construct(
                "CONSTRUCT { <" + classURI + "> <" + RDFS.subClassOf + "> <" + superclassURI + "> } WHERE " +
                        "{ <" + classURI + "> <" + RDFS.subClassOf + "> <" + superclassURI + "> }");
        return (ask.size() > 0);
    }
    
    protected boolean isProcess(String classURI, SparqlEndpoint sparqlEndpoint) {
        return isSubclass(classURI, PROCESS, sparqlEndpoint);
    }
    
    protected boolean isRole(String classURI, SparqlEndpoint sparqlEndpoint) {
        return isSubclass(classURI, ROLE, sparqlEndpoint);
    }
    
    protected boolean isRelationship(String classURI, SparqlEndpoint sparqlEndpoint) {
        return isSubclass(classURI, RELATIONSHIP, sparqlEndpoint);
    }
    
    protected String getFauxPropertyPattern(MergeRule rule, 
            MergeRuleAtom atom, FauxPropertyContext ctx, 
            SparqlEndpoint sparqlEndpoint) {
        if(ctx.getQualifiedBy().startsWith(VCARD)) {
            String vCardPattern = vCardPattern(rule, ctx);
            if (vCardPattern != null) {
                return vCardPattern;
            }
        } else if (isRole(ctx.getQualifiedBy(), sparqlEndpoint)) {
            if(isProcess(rule.getMergeClassURI(), sparqlEndpoint)) {
                return rolePatternForProcess(rule, atom, ctx);   
            } else {
                return rolePattern(rule, atom, ctx);
            }
        } else if (isRelationship(ctx.getQualifiedBy(), sparqlEndpoint)) {
            return relationshipPattern(rule, atom, ctx);
        } else if ((VIVO + "DateTimeValue").equals(ctx.getQualifiedBy())) {
            return dateTimeValuePattern(rule, atom, ctx);
        } else if ((VIVO + "DateTimeInterval").equals(ctx.getQualifiedBy())) {
            return dateTimeIntervalPattern(rule, atom, ctx);
        }
        return standardFauxPropertyPattern(rule, atom, ctx);        
    }
 
    protected String standardFauxPropertyPattern(MergeRule rule, 
            MergeRuleAtom atom, FauxPropertyContext ctx) {
        String pattern = //  "{" +
                "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?x <" + ctx.getPropertyURI() + "> ?value . \n" +
                "    ?value a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?value <" + OWL.sameAs.getURI() + "> ?value2 . \n" +
                "    ?y <" + ctx.getPropertyURI() + "> ?value2 . \n" +
                "    ?y a <" + rule.getMergeClassURI()  + "> . \n" ;  
        return pattern;
    }
    
    protected String rolePattern(MergeRule rule, MergeRuleAtom atom, 
            FauxPropertyContext ctx) {
        String pattern = "    ?role1 <" + REALIZED_IN + "> ?value . \n" +
                "    ?role2 <" + REALIZED_IN + "> ?value . \n" +
                "    ?role1 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?x <" + ctx.getPropertyURI() + "> ?role1 . \n" +
                "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?role2 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?y <" + ctx.getPropertyURI() + "> ?role2 . \n" +
                "    ?y a <" + rule.getMergeClassURI()  + "> . \n" ;
        if(atom.getMergeDataPropertyURI() != null) {
            pattern += "    ?role1 <" + atom.getMergeDataPropertyURI() + "> ?dataPropertyValue . \n" +
                    "    ?role2 <" + atom.getMergeDataPropertyURI() + "> ?dataPropertyValue . \n";
        }
        return pattern;
    }
    
    protected String rolePatternForProcess(MergeRule rule, MergeRuleAtom atom, 
            FauxPropertyContext ctx) {
        String pattern = "    ?role1 <" + INHERES_IN + "> ?value . \n" +
                "    ?role2 <" + INHERES_IN + "> ?value . \n" +
                "    ?role1 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?x <" + ctx.getPropertyURI() + "> ?role1 . \n" +
                "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?role2 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?y <" + ctx.getPropertyURI() + "> ?role2 . \n" +
                "    ?y a <" + rule.getMergeClassURI()  + "> . \n" ;
        if(atom.getMergeDataPropertyURI() != null) {
            pattern += "    ?role1 <" + atom.getMergeDataPropertyURI() + "> ?dataPropertyValue . \n" +
                    "    ?role2 <" + atom.getMergeDataPropertyURI() + "> ?dataPropertyValue . \n";
        }
        return pattern;
    }
    
    protected String relationshipPattern(MergeRule rule, MergeRuleAtom atom, 
            FauxPropertyContext ctx) {
        String pattern =                     
                "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?x <" + ctx.getPropertyURI() + "> ?relationship1 . \n" +
                "    ?relationship1 a <" + ctx.getQualifiedBy() + "> . \n" +                        
                "    ?relationship1 <" + VIVO + "relates> ?value . \n" +     
                "    FILTER NOT EXISTS { ?value a <" + rule.getMergeClassURI() + "> } . \n";
        if(atom.getMergeDataPropertyURI() != null) {
            pattern += "    ?relationship1 <" + atom.getMergeDataPropertyURI() + "> ?dataPropertyValue . \n";
        }
        pattern +=
                "    ?value <" + OWL.sameAs.getURI() + "> ?value2 . \n" +
                "    ?relationship2 <" + VIVO + "relates> ?value2 . \n";
        if(atom.getMergeDataPropertyURI() != null) {
            pattern += "    ?relationship2 <" + atom.getMergeDataPropertyURI() + "> ?dataPropertyValue . \n";
        }        
        pattern +=
                "    FILTER(?relationship1 != ?relationship2) \n" +
                "    ?relationship2 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?y <" + ctx.getPropertyURI() + "> ?relationship2 . \n" +
                "    ?y a <" + rule.getMergeClassURI()  + "> . \n" ;                 
        return pattern;
    }
    
    protected String dateTimeValuePattern(MergeRule rule, MergeRuleAtom atom, 
            FauxPropertyContext ctx) {
        String pattern = 
                "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?x <" + ctx.getPropertyURI() + "> ?dtv1 . \n" +
                "    ?dtv1 <" + VIVO + "dateTime> ?dateTime . \n" +
                "    ?dtv2 <" + VIVO + "dateTime> ?dateTime . \n" +
                "    ?dtv1 <" + VIVO + "dateTimePrecision> ?dateTimePrecision . \n" +
                "    ?dtv2 <" + VIVO + "dateTimePrecision> ?dateTimePrecision . \n" +                
                "    ?y <" + ctx.getPropertyURI() + "> ?dtv2 . \n" +
                "    ?y a <" + rule.getMergeClassURI()  + "> . \n" +
                "    ?dtv1 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?dtv2 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    FILTER(?dtv1 != ?dtv2) \n" ;
        return pattern;
    }
    
    protected String dateTimeIntervalPattern(MergeRule rule, MergeRuleAtom atom, 
            FauxPropertyContext ctx) {
        String pattern = 
                "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?x <" + ctx.getPropertyURI() + "> ?dti1 . \n" +
                "    ?dti1 <" + VIVO + "start> ?start1 . \n" +
                "    ?start1 <" + VIVO + "dateTime> ?startDateTime . \n" +
                "    ?start2 <" + VIVO + "dateTime> ?startDateTime . \n" +
                "    ?start1 <" + VIVO + "dateTimePrecision> ?startDateTimePrecision . \n" +
                "    ?start2 <" + VIVO + "dateTimePrecision> ?startTimePrecision . \n" +
                "    ?dti1 <" + VIVO + "end> ?end1 . \n" +
                "    ?end1 <" + VIVO + "dateTime> ?endDateTime . \n" +
                "    ?end2 <" + VIVO + "dateTime> ?endDateTime . \n" +
                "    ?end1 <" + VIVO + "dateTimePrecision> ?endDateTimePrecision . \n" +
                "    ?end2 <" + VIVO + "dateTimePrecision> ?endTimePrecision . \n" +
                "    ?dti2 <" + VIVO + "start> ?start2 . \n" +
                "    ?dti2 <" + VIVO + "end> ?end2 . \n" +
                "    ?y <" + ctx.getPropertyURI() + "> ?dti2 . \n" +
                "    ?y a <" + rule.getMergeClassURI()  + "> . \n" +
                "    ?dti1 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?dti2 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    FILTER(?dti1 != ?dti2) \n" ;
        return pattern;
    }
    
    protected String vCardPattern(MergeRule rule, FauxPropertyContext ctx) {
        String connectingProperty;
        String datatypeProperty;
        if((VCARD + "Telephone").equals(ctx.getQualifiedBy()) 
                || (VCARD + "Fax").equals(ctx.getQualifiedBy())) {
            connectingProperty = VCARD + "hasTelephone";
            datatypeProperty = VCARD + "telephone";
        } else if ((VCARD + "Work").equals(ctx.getQualifiedBy()) 
                || (VCARD + "Email").equals(ctx.getQualifiedBy())) {
            connectingProperty = VCARD + "hasEmail";
            datatypeProperty = VCARD + "email";
        } else if ((VCARD + "URL").equals(ctx.getQualifiedBy())) { 
            connectingProperty = VCARD + "hasURL";
            datatypeProperty = VCARD + "url";
        } else {
            return null;
        }
        return  
                "{" +
                "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?x <" + ctx.getPropertyURI() + "> ?vcard . \n" +
                "    ?vcard <" + connectingProperty +"> ?box . \n" +
                "    ?box a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?box <" + datatypeProperty + "> ?value . \n" +
                "    ?boxy <" + datatypeProperty + "> ?value . \n" +
                "    ?boxy a <" +ctx.getQualifiedBy()  + "> . \n" +
                "    ?vcardy <" + connectingProperty +"> ?boxy . \n" +
                "    ?y <" + ctx.getPropertyURI() + "> ?vcardy . \n" +
                "    ?y a <" + rule.getMergeClassURI() + "> . \n" +                
                "} \n";
    }
    
    protected boolean isFauxPropertyContext(String mergeObjectPropertyURI, 
            Model fauxPropertyContextModel) {
        return fauxPropertyContextModel.contains(
                fauxPropertyContextModel.getResource(mergeObjectPropertyURI), 
                        RDF.type, fauxPropertyContextModel.getResource(
                                APPLICATION_CONTEXT_NS + "ConfigContext"));
    }
    
    protected Model retrieveMergeRulesFromEndpoint(SparqlEndpoint endpoint) {
        String queryStr = "DESCRIBE ?x WHERE { \n" 
                + "    { ?x a <" + MERGERULE + "> } \n" 
                + "UNION \n" 
                + "    { ?x a <" + MERGERULEATOM + "> } \n"
                + "FILTER NOT EXISTS { ?x <" + DISABLED + "> true } \n"   
                + "} \n";
        return endpoint.construct(queryStr);
    }
    
    /**
     * @param endpoint the SPARQL endpoint to query
     * @return model containing symmetric closure of differentFrom statements
     *         found in the supplied endpoint
     * @return null if endpoint is null
     */
    protected Model getDifferentFromModel(SparqlEndpoint endpoint) {
        if(endpoint == null) {
            return null;
        }
        String queryStr = "CONSTRUCT { ?x <" + OWL.differentFrom.getURI() + "> ?y . \n" 
                + "    ?y <" + OWL.differentFrom.getURI() + "> ?x . \n" 
                + "} WHERE { \n" 
                + "    ?x <" + OWL.differentFrom.getURI() + "> ?y \n" 
                + "} \n";
        return endpoint.construct(queryStr);
    }
    
    protected MergeRule getMergeRule(String ruleURI, Model model) {          
        MergeRule mergeRule = new MergeRule();
        mergeRule.setURI(ruleURI);
        Integer priority = getInteger(ruleURI, PRIORITY, model);
        if(priority == null) {
            mergeRule.setPriority(0);
        } else {
            mergeRule.setPriority(priority);
        }
        mergeRule.setMergeClassURI(getString(ruleURI, MERGERULECLASS, model));
        ParameterizedSparqlString atomQuery = new ParameterizedSparqlString(
                "SELECT ?atom WHERE { \n" +
                "  ?rule <" + HASATOM + "> ?atom . \n" +
                "  OPTIONAL { ?atom <" + PRIORITY +"> ?priority } \n" +
                "  FILTER(!BOUND(?rank) || (?rank >= 0)) \n" +
                "  FILTER NOT EXISTS { ?atom <" + DISABLED + "> true } \n" +
                "} ORDER BY ?rank");
        atomQuery.setIri("rule", ruleURI);
        QueryExecution qe = QueryExecutionFactory.create(atomQuery.toString(), model);
        try {
            ResultSet rs = qe.execSelect();
            while(rs.hasNext()) {
                QuerySolution qsoln = rs.next();
                if(qsoln.get("atom").isURIResource()) {
                    mergeRule.addAtom(getAtom(
                            qsoln.get("atom").asResource().getURI(), model));
                }
            }
        } finally {
            if(qe != null) {
                qe.close();
            }
        }       
        return mergeRule;
    }
    
    protected MergeRuleAtom getAtom(String atomURI, Model model) {
        MergeRuleAtom atom = new MergeRuleAtom();
        atom.setURI(atomURI);
        String dataPropertyURI = getString(atomURI, MERGEATOMDATAPROPERTY, model);
        if(dataPropertyURI != null) {
            atom.setMergeDataPropertyURI(dataPropertyURI);
        }
        String objectPropertyURI = getString(atomURI, MERGEATOMOBJECTPROPERTY, model);
        if(objectPropertyURI != null){
            atom.setMergeObjectPropertyURI(objectPropertyURI);                
        }
        Integer matchDegree = getInteger(atomURI, MATCHDEGREE, model);
        if(matchDegree == null) {
            atom.setMatchDegree(100);
        } else {
            atom.setMatchDegree(matchDegree);
        }
        return atom;
    }
    
    private String getString(String subjectURI, String predicateURI, Model model) {
        Resource subject = ResourceFactory.createResource(subjectURI);
        Property predicate = ResourceFactory.createProperty(predicateURI);
        StmtIterator sit = model.listStatements(subject, predicate, (RDFNode) null);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            RDFNode object = stmt.getObject();
            if(object.isLiteral()) {
                return object.asLiteral().getLexicalForm();
            } else if (object.isURIResource()) {
                return object.asResource().getURI();
            }
        }
        return null;
    }
    
    private Integer getInteger(String subjectURI, String predicateURI, Model model) {
        String object = getString(subjectURI, predicateURI, model);
        try {
            return Integer.parseInt(object, 10);
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    protected class MergeRuleAtom {
        private String URI;
        private String mergeDataPropertyURI;
        private String mergeObjectPropertyURI;
        private int matchDegree;
        
        public String getURI() {
            return this.URI;
        }
        public void setURI(String URI) {
            this.URI = URI;
        }
        
        public String getMergeDataPropertyURI() {
            return this.mergeDataPropertyURI;
        }
        public void setMergeDataPropertyURI(String mergePropertyURI) {
            this.mergeDataPropertyURI = mergePropertyURI;
        }
        
        public String getMergeObjectPropertyURI() {
            return this.mergeObjectPropertyURI;
        }
        public void setMergeObjectPropertyURI(String mergePropertyURI) {
            this.mergeObjectPropertyURI = mergePropertyURI;
        }
        
        public int getMatchDegree() {
            return this.matchDegree;
        }
        public void setMatchDegree(int matchDegree) {
            this.matchDegree = matchDegree;
        }
    }
    
    protected class MergeRule {
        
        private String URI;
        private String mergeClassURI;
        private int priority;
        private List<MergeRuleAtom> atoms = new ArrayList<MergeRuleAtom>();
        
        public String getURI() {
            return this.URI;
        }
        public void setURI(String URI) {
            this.URI = URI;
        }
        
        public String getMergeClassURI() {
            return this.mergeClassURI;
        }
        public void setMergeClassURI(String mergeClassURI) {
            this.mergeClassURI = mergeClassURI;
        }
        
        public int getPriority() {
            return this.priority;
        }
        public void setPriority(int priority) {
            this.priority = priority;
        }
        
        public List<MergeRuleAtom> getAtoms() {
            return this.atoms;
        }
        public void addAtom(MergeRuleAtom atom) {
            this.atoms.add(atom);
        }
    }    
    
    protected class FauxPropertyContext {
        
        private String contextURI;
        private String propertyURI;
        private String qualifiedBy;
       
        public String getContextURI() {
            return this.contextURI;
        }
        public void setContextURI(String contextURI) {
            this.contextURI = contextURI;
        }
        
        public String getPropertyURI() {
            return propertyURI;
        }
        public void setPropertyURI(String propertyURI) {
            this.propertyURI = propertyURI;
        }
        
        public String getQualifiedBy() {
            return this.qualifiedBy;
        }
        public void setQualifiedBy(String qualifiedBy) {
            this.qualifiedBy = qualifiedBy;
        }    
    }
    
    protected FauxPropertyContext getFauxPropertyContext(String contextURI, Model contextModel) {
        FauxPropertyContext context = new FauxPropertyContext();
        context.setContextURI(contextURI);
        context.setPropertyURI(getString(contextURI, CONFIG_CONTEXT_FOR, contextModel));
        context.setQualifiedBy(getString(contextURI, QUALIFIED_BY, contextModel));
        return context;
    }
    
    private List<String> getMergeRuleURIs(String dataSourceURI) {
        List<String> mergeRuleURIs = new ArrayList<String>();
        String queryStr = "SELECT ?x WHERE { \n" +
                 "    <" + dataSourceURI + "> <" + HASMERGERULE + "> ?x \n" +
                 "    FILTER NOT EXISTS { ?x <" + DISABLED + "> true } \n" + 
                 "} \n";
        ResultSet rs = getSparqlEndpoint().getResultSet(queryStr);
        while(rs.hasNext()) {
            QuerySolution qsoln = rs.next();
            RDFNode n = qsoln.get("x");
            if(n.isURIResource()) {
                mergeRuleURIs.add(n.asResource().getURI());
            }
        }
        return mergeRuleURIs;
    }

    @Override
    public Model getResult() {
        return this.result;
    }
    
    protected Model getFuzzySameAs(MergeRule rule, MergeRuleAtom atom, 
            Model fauxPropertyContextModel, int windowSize) {
        Model fuzzySameAs = ModelFactory.createDefaultModel();
        String fuzzyQueryStr = getFuzzyQueryStr(
                rule, atom, fauxPropertyContextModel);
        log.info("Processing fuzzy query " + fuzzyQueryStr);
        ResultSet rs = getSparqlEndpoint().getResultSet(fuzzyQueryStr);
        log.debug("Processing matches");
        LinkedList<QuerySolution> window = new LinkedList<QuerySolution>();
        window = prepopulate(window, rs, windowSize);
        log.debug("Window prepopulated.");
        fuzzySameAs.add(getMatches(window, atom.getMatchDegree()));
        log.debug("Initial window processed");
        if(!rs.hasNext()) {
            return fuzzySameAs;
        }
        int pos = windowSize / 2;
        while(rs.hasNext()) {
            window.add(rs.next()); // add one
            window.poll(); // drop one
            fuzzySameAs.add(getMatches(window, atom.getMatchDegree(), pos));
        }
        log.debug("Result set processed.");
        fuzzySameAs.add(getMatches(window, atom.getMatchDegree()));
        log.debug("Final window processed.");
        log.debug(fuzzySameAs.size() + " fuzzy sameAs results");
        return fuzzySameAs;
    }
    
    /**
     * Get fuzzy string matches formulated as sameAs statements about their
     * respective individuals.
     * @param qsoln
     * @param matchDegree
     * @param firstInPair - integer position of first individual in comparison
     */
    protected Model getMatches(LinkedList<QuerySolution> window, 
            int matchDegree, Integer firstInPair) {
        if((firstInPair + 1) > window.size()) {
            throw new RuntimeException("firstInPair " + firstInPair 
                    + " out of bounds. Window size is only " + window.size());
        }
        Model matches = ModelFactory.createDefaultModel();
        int i = -1;
        QuerySolution qsoln1 = window.get(firstInPair);
        while(i < (window.size() -1)) {
            i++;
            log.debug("i=" + i + "; windowSize=" + window.size());
            if(i == firstInPair) {
                continue;
            }
            QuerySolution qsoln2 = window.get(i);
            if(matchFuzzily(qsoln1, qsoln2, matchDegree)) {
                RDFNode node1 = qsoln1.get("x");
                if(node1.isURIResource()) {
                    Resource res1 = node1.asResource();
                    RDFNode node2 = qsoln2.get("x");
                    if(node2.isURIResource()) {
                        Resource res2 = node2.asResource();
                        matches.add(res1, OWL.sameAs, res2);
                    }
                }
            }
        }
        return matches;
    }
    
    protected Model getMatches(LinkedList<QuerySolution> window, int matchDegree) {
        Model matches = ModelFactory.createDefaultModel();
        for(int i = 0; i < window.size(); i++) {
            matches.add(getMatches(window, matchDegree, i));
        }
        return matches;
    }
    
    private boolean matchFuzzily(QuerySolution qsoln1, QuerySolution qsoln2, 
            int matchDegree) {
        RDFNode n1 = qsoln1.get("value");
        if(n1.isLiteral()) {
            String string1 = n1.asLiteral().getLexicalForm();
            RDFNode n2 = qsoln2.get("value");
            if(n2.isLiteral()) {
                String string2 = n2.asLiteral().getString();
                // TODO express some other way
                // matchDegree 19 is magic value for initial-only match
                if(matchDegree == 19) {
                    return initialsMatch(string1, string2);
                }
                int distance = distance(string1, string2);
                if(distance == 0) {
                    return true;
                }
                float avglen = ((string1.length() + string2.length()) / 2);
                if(avglen == 0) {
                    return false;
                }
                if(distance >= avglen) {
                    return false;
                } 
                float score = ((1 - (distance / avglen))  * 100);
                log.debug("Match score: " + score);
                boolean match = (score >= matchDegree);
                log.debug("Required score " + matchDegree + " (" + (match ? "PASS" : "FAIL") + ")");
                return match;
            }
        }
        return false;
    }
    
    private boolean initialsMatch(String string1, String string2) {
        if(string1.isEmpty() || string2.isEmpty()) {
            return false;
        } else {
            return (string1.charAt(0) == string2.charAt(0));
        }
    }
    
    private LinkedList<QuerySolution> prepopulate(
            LinkedList<QuerySolution> window, ResultSet rs, int windowSize) {
        int limit = windowSize;
        while(limit > 0 && rs.hasNext()) {
            window.add(rs.next());
            limit--;
        }
        return window;
    }
    
    protected String getFuzzyQueryStr(MergeRule rule, MergeRuleAtom atom, Model fauxPropertyContextModel) {
        if(isFauxPropertyContext(atom.getMergeObjectPropertyURI(), 
                fauxPropertyContextModel)) {
            FauxPropertyContext ctx = getFauxPropertyContext(
                    atom.getMergeObjectPropertyURI(), 
                    fauxPropertyContextModel);
            if(!ctx.getQualifiedBy().startsWith(VCARD)) {
                throw new RuntimeException("Invalid atom " + atom.getURI() 
                + " : fuzzy matches on non-vCard faux properties not permitted");
            }
            String fuzzyVCardQueryStr = getFuzzyVCardQueryStr(rule, atom, ctx);
            if(fuzzyVCardQueryStr != null) {
                return fuzzyVCardQueryStr;
            }
        } 
        String queryStr;
        if(atom.getMergeDataPropertyURI().startsWith(VCARD) 
                && atom.getMergeDataPropertyURI().endsWith("Name")) {
            queryStr = getSingleEndedVcardNameQuery(rule, atom);
        } else {
            queryStr = "SELECT ?x ?value WHERE { \n" +
                "    ?x a <" + rule.getMergeClassURI() + "> ." +
                "    ?x <" + atom.getMergeDataPropertyURI() + "> ?val \n" +
                "    BIND(LCASE(?val) AS ?value) \n" +
                "} ORDER BY ?value \n";
        }
        return queryStr;
    }
    
    protected String getFuzzyVCardQueryStr(MergeRule rule, MergeRuleAtom atom, 
            FauxPropertyContext ctx) {
        String connectingProperty;
        String datatypeProperty;
        if((VCARD + "Telephone").equals(ctx.getQualifiedBy()) 
                || (VCARD + "Fax").equals(ctx.getQualifiedBy())) {
            connectingProperty = VCARD + "hasTelephone";
            datatypeProperty = VCARD + "telephone";
        } else if ((VCARD + "Work").equals(ctx.getQualifiedBy()) 
                || (VCARD + "Email").equals(ctx.getQualifiedBy())) {
            connectingProperty = VCARD + "hasEmail";
            datatypeProperty = VCARD + "email";
        } else if ((VCARD + "URL").equals(ctx.getQualifiedBy())) { 
            connectingProperty = VCARD + "hasURL";
            datatypeProperty = VCARD + "url";
        } else {
            return null;
        }
        return  
                "SELECT ?x ?value WHERE {" +
                "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?x <" + ctx.getPropertyURI() + "> ?vcard . \n" +
                "    ?vcard <" + connectingProperty +"> ?box . \n" +
                "    ?box a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?box <" + datatypeProperty + "> ?value . \n" +
            "} \n";
    }
    
    private int distance(String string1, String string2) {  
        if(string1 == null || string2 == null) {
            return Integer.MAX_VALUE;
        }
        int distance = ld.apply(string1.toLowerCase(), string2.toLowerCase());
        log.debug("Case-insensitive Levenshtein distance " + distance + 
                " for " + string1 + " , " + string2);
        return distance;
    }
    
    protected Model getRelationshipSameAs() {
        Model m = ModelFactory.createDefaultModel();
        String relationshipListQuery = "SELECT ?x WHERE { \n" +
                " { ?x a <" + VIVO + "Position> } \n" +
                "   UNION \n" +
                " { ?x a <" + VIVO + "Authorship> } \n" +
                "} \n";
        ResultSet rs = getSparqlEndpoint().getResultSet(relationshipListQuery);
        int count = 0;
        while(rs.hasNext()) {
            QuerySolution qsoln = rs.next();
            RDFNode xnode = qsoln.get("x");
            if(xnode.isURIResource()) {
                String x = xnode.asResource().getURI();
                log.debug("Processing x= " + x);
                String query = "CONSTRUCT { ?x1 <"+ OWL.sameAs.getURI() + "> ?y1 . \n" +
                        "    ?y1 <"+ OWL.sameAs.getURI() + "> ?x1 . \n" +
                        "} WHERE { \n" +
                        "    <" + x + "> a <" + VIVO + "Relationship> . \n" +
                        "    <" + x + "> <" + VIVO +"relates> ?a . \n" +
                        "    <" + x + "> <" + VIVO +"relates> ?b . \n" +
                        "    <" + x + "> <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> ?mst . \n" +
                        " FILTER NOT EXISTS { <" + x + "> <" + VIVO + "dateTimeInterval> ?dti } \n" +
                        " FILTER NOT EXISTS { <" + x + "> <" + RDFS.label + "> ?label } \n" +
                        " FILTER (?a != ?b) \n" +
                        "    ?a <" + OWL.sameAs.getURI() + "> ?a1 . \n" +
                        "    ?b <" + OWL.sameAs.getURI() + "> ?b1 . \n" +
                        "    ?y <" + VIVO +"relates> ?b1 . \n" +
                        "    ?y <" + VIVO +"relates> ?a1 . \n" +
                        "    ?y a <" + VIVO + "Relationship> . \n" +
                        "    ?y <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> ?mst . \n" +
                        " FILTER NOT EXISTS { ?y <" + VIVO + "dateTimeInterval> ?dti } \n" +
                        " FILTER NOT EXISTS { ?y <" + RDFS.label + "> ?label } \n" +  
                        " FILTER (<" + x + "> != ?y) \n" +  
                        " <" + x + "> <" + OWL.sameAs.getURI() + "> ?x1 . \n" +
                        " ?y <" + OWL.sameAs.getURI() + "> ?y1 . \n" +
                        "} \n";
                m.add(getSparqlEndpoint().construct(query));                     
            }
            count++;
            if(count % 1000 == 0) {
                log.info("Processed " + count + " relationships");
            }
        }
        return m;
    }
    
    protected Model getRoleSameAs() {
        Model m = ModelFactory.createDefaultModel();
        String relationshipListQuery = "SELECT ?x WHERE { \n" +
                " ?x a <" + ROLE + "> \n" +
                "} \n";
        ResultSet rs = getSparqlEndpoint().getResultSet(relationshipListQuery);
        int count = 0;
        while(rs.hasNext()) {
            QuerySolution qsoln = rs.next();
            RDFNode xnode = qsoln.get("x");
            if(xnode.isURIResource()) {
                String x = xnode.asResource().getURI();
                log.debug("Processing x= " + x);
                String query = "CONSTRUCT { ?x1 <"+ OWL.sameAs.getURI() + "> ?y1 . \n" +
                        "    ?y1 <"+ OWL.sameAs.getURI() + "> ?x1 . \n" +
                        "} WHERE { \n" +
                        "    <" + x + "> a <" + ROLE + "> . \n" +
                        "    <" + x + "> <" + INHERES_IN + "> ?a . \n" +
                        "    <" + x + "> <" + REALIZED_IN + "> ?b . \n" +
                        "    <" + x + "> <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> ?mst . \n" +
                        " FILTER NOT EXISTS { <" + x + "> <" + VIVO + "dateTimeInterval> ?dti } \n" +
                        " FILTER NOT EXISTS { <" + x + "> <" + RDFS.label + "> ?label } \n" +
                        " FILTER (?a != ?b) \n" +
                        "    ?a <" + OWL.sameAs.getURI() + "> ?a1 . \n" +
                        "    ?b <" + OWL.sameAs.getURI() + "> ?b1 . \n" +
                        "    ?y <" + REALIZED_IN + "> ?b1 . \n" +
                        "    ?y <" + INHERES_IN + "> ?a1 . \n" +
                        "    ?y a <" + ROLE + "> . \n" +
                        "    ?y <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> ?mst . \n" +
                        " FILTER NOT EXISTS { ?y <" + VIVO + "dateTimeInterval> ?dti } \n" +
                        " FILTER NOT EXISTS { ?y <" + RDFS.label + "> ?label } \n" +  
                        " FILTER (<" + x + "> != ?y) \n" +  
                        " <" + x + "> <" + OWL.sameAs.getURI() + "> ?x1 . \n" +
                        " ?y <" + OWL.sameAs.getURI() + "> ?y1 . \n" +
                        "} \n";
                m.add(getSparqlEndpoint().construct(query));                     
            }
            count++;
            if(count % 1000 == 0) {
                log.info("Processed " + count + " roles");
            }
        }
        return m;
    }
    
    protected Model getVcardSameAs(SparqlEndpoint endpoint) {
        Model m = endpoint.construct(loadQuery(SPARQL_RESOURCE_DIR + "mergeVcards.rq"));
        log.info("Constructed " + m.size() + " vcard-related sameAs triples");
        return m;
    }
  
    protected Model getVcardPartsSameAs(SparqlEndpoint endpoint) {
        Model m = endpoint.construct(loadQuery(SPARQL_RESOURCE_DIR + "mergeVcardParts.rq"));
        log.info("Constructed " + m.size() + " vcard-parts-related sameAs triples");
        return m;
    }
  
    /*
     * Find the minimum size of the comparison window for fuzzy string matches.
     * Because we will likely want to do initial-only matches on vcard:givenName,
     * this value needs to be at least as large as the letter that begins the
     * greatest number of vcard:givenName values.
     */
    protected int getWindowSize(SparqlEndpoint sparqlEndpoint) {
        if(true) {
            return 100; // the rest gets expensive
        }
        // TODO revisit
        int windowSize = DEFAULT_WINDOW_SIZE;
        for(char i = 'A'; i < 'Z'; i++) {
            String query = "SELECT (COUNT(DISTINCT ?x) AS ?count) WHERE { \n" +
                           "    ?x <" + VCARD + "givenName> ?value \n" +
                           "    FILTER(REGEX(STR(?value), \"^" + i + "\", \"i\")) \n" +
                           "} \n";
            ResultSet rs = sparqlEndpoint.getResultSet(query);
            while(rs.hasNext()) {
                QuerySolution qsoln = rs.next();
                RDFNode node = qsoln.get("count");
                if(node.isLiteral()) {
                    try {
                        int count = node.asLiteral().getInt();
                        if(count > windowSize) {
                            windowSize = count;
                        }
                    } catch (Exception e) {
                        log.error(e, e);
                    }
                }
            }
        }
        windowSize = windowSize + 3;  // pad for any possible off-by-one errors
        log.info("Using fuzzy sting comparison window size of " + windowSize);
        return windowSize;
    }
    
    private class AffectedClassRuleComparator implements Comparator<MergeRule> {

        private SparqlEndpoint sparqlEndpoint;
        
        public AffectedClassRuleComparator(SparqlEndpoint sparqlEndpoint) {
            this.sparqlEndpoint = sparqlEndpoint;
        }
        
        public int compare(MergeRule a, MergeRule b) {
            int aRank = rankAffectedClass(a.getMergeClassURI());
            int bRank = rankAffectedClass(b.getMergeClassURI());
            if(aRank != bRank) {
                return aRank - bRank;
            } else {
                if(a.getMergeClassURI() == null) {
                    return -1;
                } else if (b.getMergeClassURI() == null) {
                    return 1;
                } else {
                    return a.getMergeClassURI().compareTo(b.getMergeClassURI());
                }
            }
        }
        
        private int rankAffectedClass(String classURI) {
            if(classURI == null) {
                return 0;
            } else {
                if(isSubclass(classURI, BIBO_COLLECTION, this.sparqlEndpoint)) {
                    return -2;
                } else if (isSubclass(classURI, BIBO_DOCUMENT, this.sparqlEndpoint)) {
                    return -1;
                } else if (isSubclass(classURI, FOAF_ORGANIZATION, this.sparqlEndpoint)) {
                    return 1;
                } else if (isSubclass(classURI, FOAF_PERSON, this.sparqlEndpoint)) {
                    return 2;
                } else {
                    return 0;
                }
            }
        }
        
    }
    
}
