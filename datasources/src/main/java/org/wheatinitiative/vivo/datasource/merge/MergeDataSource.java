package org.wheatinitiative.vivo.datasource.merge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.DataSourceBase;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class MergeDataSource extends DataSourceBase implements DataSource {

    private static final Log log = LogFactory.getLog(MergeDataSource.class);
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
    private static final String MERGESOURCE = ADMIN_APP_TBOX + "MergeDataSource";
    private static final String MERGERULE = ADMIN_APP_TBOX + "MergeRule";
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
    
    private Model result = ModelFactory.createDefaultModel();
    protected LevenshteinDistance ld = LevenshteinDistance.getDefaultInstance();
    
    @Override
    protected void runIngest() {
        String dataSourceURI = this.getConfiguration().getURI();
        log.info("Starting merge " + dataSourceURI);
        Model rulesModel = retrieveMergeRulesFromEndpoint(this.getSparqlEndpoint());
        Model fauxPropertyContextModel = ModelFactory.createDefaultModel();
        String fauxPropertyModelURI = this.getConfiguration().getServiceURI() 
                + "fauxPropertyContexts";
        log.info("Retrieving faux property contexts from " + fauxPropertyModelURI);
        fauxPropertyContextModel.read(fauxPropertyModelURI);
        log.info(fauxPropertyContextModel.size() + " faux property context statements.");
        log.info("Merging relationships");
        result.add(getRelationshipSameAs()); 
        log.info(result.size() + " after merged relationships");
        Map<String, Long> statistics = new HashMap<String, Long>();
        for(String mergeRuleURI : getMergeRuleURIs(dataSourceURI)) {
            // TODO flush to endpoint and repeat rules until quiescent?
            long previousSize = result.size();
            log.info("Processing rule " + mergeRuleURI);            
            MergeRule rule = getMergeRule(mergeRuleURI, rulesModel); 
            Model ruleResult = getSameAs(rule, fauxPropertyContextModel, sparqlEndpoint);
            filterObviousResults(ruleResult);
            result.add(ruleResult);
            getSparqlEndpoint().clearGraph(mergeRuleURI);
            getSparqlEndpoint().writeModel(ruleResult, mergeRuleURI);
            statistics.put(mergeRuleURI, ruleResult.size() - previousSize);
            log.info("Rule results size: " + ruleResult.size());
        }
        log.info("======== Final Results ========");
        for(String ruleURI : statistics.keySet()) {            
            log.info("Rule " + ruleURI + " added " + statistics.get(ruleURI));
        }
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
            SparqlEndpoint sparqlEndpoint) {
        Model sameAsModel = null;
        for (MergeRuleAtom atom : rule.getAtoms()) {
            log.debug("Processing atom " + atom.getMergeDataPropertyURI() + " ; " 
                    + atom.getMergeObjectPropertyURI() + " ; " 
                    + atom.getMatchDegree());
            if(atom.getMatchDegree() < 100) {
                sameAsModel = join(sameAsModel, getFuzzySameAs(
                        rule, fauxPropertyContextModel));
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
                            "    ?x <" + OWL.sameAs.getURI() + "> ?y \n" +
                            "} WHERE { \n"
                            + queryStr + "} \n";
                    log.info("Generated sameAs query: \n" + queryStr);                    
                    sameAsModel = join(sameAsModel, sparqlEndpoint.construct(
                            queryStr));
                }
            }
        }         
        return sameAsModel;
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
        return  "SELECT ?x ?value WHERE { \n" +
                "    ?x a <" + rule.getMergeClassURI() + "> . \n" +
                "    ?x <" + HASCONTACTINFO + "> ?vcard . \n" +
                "    ?vcard <" + VCARD + "hasName> ?name . \n" +
                "    ?name <" + atom.getMergeDataPropertyURI() + "> ?value . \n" +
                "} ORDER BY ?value \n" ;
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
                    "    ?y <" + atom.getMergeObjectPropertyURI() + "> ?value . \n" +
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
                "    ?y <" + ctx.getPropertyURI() + "> ?value . \n" +
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
                "    ?relationship1 <" + VIVO + "relates> ?value . \n" +                     
                "    ?relationship2 <" + VIVO + "relates> ?value . \n" +
                "    FILTER NOT EXISTS { ?value a <" + rule.getMergeClassURI() + "> } . \n" +
                "    ?relationship1 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?relationship2 a <" + ctx.getQualifiedBy() + "> . \n" +
                "    ?y <" + ctx.getPropertyURI() + "> ?relationship2 . \n" +
                "    ?y a <" + rule.getMergeClassURI()  + "> . \n" ;         
        if(atom.getMergeDataPropertyURI() != null) {
            pattern += "    ?relationship1 <" + atom.getMergeDataPropertyURI() + "> ?dataPropertyValue . \n" +
                    "    ?relationship2 <" + atom.getMergeDataPropertyURI() + "> ?dataPropertyValue . \n";
        }
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
        StmtIterator atomIt = model.listStatements(
                model.getResource(ruleURI), model.getProperty(HASATOM), (RDFNode) null);
        while(atomIt.hasNext()) {
          Statement atomStmt = atomIt.next();
          if(!atomStmt.getObject().isURIResource()) {
              continue;
          } else {
              mergeRule.addAtom(getAtom(
                      atomStmt.getObject().asResource().getURI(), model));
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
    
    private static final int WINDOW_SIZE = 11;
    
    protected Model getFuzzySameAs(MergeRule rule, Model fauxPropertyContextModel) {
        Model fuzzySameAs = ModelFactory.createDefaultModel();
        boolean fuzzyUsed  = false;
        for(MergeRuleAtom atom : rule.getAtoms()) {
            if(atom.getMatchDegree() < 100) {
                fuzzyUsed = true;
                String fuzzyQueryStr = getFuzzyQueryStr(
                        rule, atom, fauxPropertyContextModel);
                log.info("Processing fuzzy query " + fuzzyQueryStr);
                ResultSet rs = getSparqlEndpoint().getResultSet(fuzzyQueryStr);
                log.debug("Processing matches");
                LinkedList<QuerySolution> window = new LinkedList<QuerySolution>();
                window = prepopulate(window, rs);
                log.debug("Window prepopulated.");
                fuzzySameAs.add(getMatches(window, atom.getMatchDegree()));
                log.debug("Initial window processed");
                if(!rs.hasNext()) {
                    return fuzzySameAs;
                }
                int pos = WINDOW_SIZE / 2;
                while(rs.hasNext()) {
                    window.add(rs.next()); // add one
                    window.poll(); // drop one
                    fuzzySameAs.add(getMatches(window, atom.getMatchDegree(), pos));
                }
                log.debug("Result set processed.");
                fuzzySameAs.add(getMatches(window, atom.getMatchDegree()));
                log.debug("Final window processed.");
            } 
        }
        if(fuzzyUsed) {
            log.debug(fuzzySameAs.size() + " fuzzy sameAs results");
        }
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
    
    private LinkedList<QuerySolution> prepopulate(
            LinkedList<QuerySolution> window, ResultSet rs) {
        int limit = WINDOW_SIZE;
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
                "    ?x <" + atom.getMergeDataPropertyURI() + "> ?value \n" +
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
        int distance = ld.apply(string1, string2);
        log.debug("Levenshtein distance " + distance + " for " + string1 + " , " + string2);
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
                String query = "CONSTRUCT { <" + x + "> <"+ OWL.sameAs + "> ?y } WHERE { \n" +
                        "    <" + x + "> a <" + VIVO + "Relationship> . \n" +
                        "    <" + x + "> <" + VIVO +"relates> ?a . \n" +
                        "    <" + x + "> <" + VIVO +"relates> ?b . \n" +
                        "    <" + x + "> <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> ?mst . \n" +
                        " FILTER NOT EXISTS { <" + x + "> <" + VIVO + "dateTimeInterval> ?dti } \n" +
                        " FILTER NOT EXISTS { <" + x + "> <" + RDFS.label + "> ?label } \n" +
                        " FILTER (?a != ?b) \n" +
                        "    ?y <" + VIVO +"relates> ?b . \n" +
                        "    ?y <" + VIVO +"relates> ?a . \n" +
                        "    ?y a <" + VIVO + "Relationship> . \n" +
                        "    ?y <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> ?mst . \n" +
                        " FILTER NOT EXISTS { ?y <" + VIVO + "dateTimeInterval> ?dti } \n" +
                        " FILTER NOT EXISTS { ?y <" + RDFS.label + "> ?label } \n" +  
                        " FILTER (<" + x + "> != ?y) \n" +  
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
    
}
