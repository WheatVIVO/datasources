package org.wheatinitiative.vivo.datasource.merge;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.DataSourceBase;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.vocabulary.OWL;

public class MergeDataSource extends DataSourceBase implements DataSource {

    private static final Log log = LogFactory.getLog(MergeDataSource.class);
    private static final String ADMIN_APP_TBOX = 
            "http://vivo.wheatinitiative.org/ontology/adminapp/";
    private static final String MERGESOURCE = ADMIN_APP_TBOX + "MergeDataSource";
    private static final String HASMERGERULE = ADMIN_APP_TBOX + "hasMergeRule";
    private static final String MERGERULECLASS = ADMIN_APP_TBOX + "mergeRuleClass";
    private static final String HASATOM = ADMIN_APP_TBOX + "hasAtom";
    private static final String MERGEATOMDATAPROPERTY = ADMIN_APP_TBOX + "mergeAtomDataProperty";
    
    private Model result = ModelFactory.createDefaultModel();
    
    @Override
    protected void runIngest() {
        String dataSourceURI = this.getConfiguration().getURI();
        log.info("Starting merge " + dataSourceURI);
        for(String mergeRuleURI : getMergeRuleURIs(dataSourceURI)) {
            log.info("Processing rule " + mergeRuleURI);
            String query = getSameAsQuery(mergeRuleURI);
            log.info(query);
            result.add(this.getSparqlEndpoint().construct(query));
            log.info("Results size: " + this.getResult().size());
        }
    }
    
    private String getSameAsQuery(String mergeRuleURI) {
        return "CONSTRUCT { \n " +
               "    ?x <" + OWL.sameAs.getURI() + "> ?y \n" +
               "} WHERE { \n" +
               "    <" + mergeRuleURI + "> <" + MERGERULECLASS + "> ?class . \n" +
               "    <" + mergeRuleURI + "> <" + HASATOM + "> ?atom . \n" +
               "    ?atom <" + MERGEATOMDATAPROPERTY + "> ?dataProperty . \n" +
               "    ?x a ?class . \n" +
               "    ?x ?dataProperty ?value . \n" +
               "    ?y ?dataProperty ?value . \n" +
               "    ?y a ?class . \n" +
               "} \n";
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
    
}
