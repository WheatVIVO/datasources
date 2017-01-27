package org.wheatinitiative.vivo.datasource.connector.prodinra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.DataSourceBase;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

public class Prodinra extends DataSourceBase implements DataSource {

    public static final Log log = LogFactory.getLog(Prodinra.class);
    
    private static final String ENDPOINT = "http://oai.prodinra.inra.fr/ft";
    private static final String METADATA_PREFIX = "oai_inra";
    private static final String PRODINRA_TBOX_NS = "http://record.prodinra.inra.fr/";
    private static final String PRODINRA_ABOX_NS = PRODINRA_TBOX_NS + "individual/";
    private static final String NAMESPACE_ETC = PRODINRA_ABOX_NS + "n";
    private static final String SPARQL_RESOURCE_DIR = "/prodinra/sparql/";
    
    private Model result;
    
    private HttpUtils httpUtils = new HttpUtils();
    private XmlToRdf xmlToRdf = new XmlToRdf();
    private RdfUtils rdfUtils = new RdfUtils();
    
    @Override
    public void runIngest() {
        try { 
            String records = listRecords();
            Model model = transformToRDF(records);
            log.info(model.size() + " statements before filtering");
            model = filter(model);
            log.info(model.size() + " statements after filtering");
            result = model;
        } catch (IOException e) {
            log.error(e, e);
        }
    }
    
    protected Model filter(Model model) {
        Model filtered = ModelFactory.createDefaultModel();
        List<Resource> relevantResources = getRelevantResources(model);
        log.info(relevantResources.size() + " relevant resources");
        for (Resource res : relevantResources) {
            filtered.add(constructPersonalSubgraph(res, model));
        }
        return filtered;
    }
    
    private Model constructPersonalSubgraph(Resource personRes, Model m) {
        Model subgraph = ModelFactory.createDefaultModel();
        Map<String, String> substitutions = new HashMap<String, String>();
        substitutions.put("\\?person", "<" + personRes.getURI() + ">");
        subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getPersonalSubgraph1.sparql", m, 
                NAMESPACE_ETC, substitutions));
        subgraph.add(constructQuery(
                SPARQL_RESOURCE_DIR + "getPersonalSubgraph2.sparql", m, 
                NAMESPACE_ETC, substitutions));
        return subgraph;
    }
    
    protected List<Resource> getRelevantResources(Model model) {
        String queryStr = loadQuery(
                SPARQL_RESOURCE_DIR + "getPersonsForSearchTerm.sparql");
        List<Resource> relevantResources = new ArrayList<Resource>();
        for (String queryTerm : getConfiguration().getQueryTerms()) {
            String query = queryStr.replaceAll("\\$TERM", queryTerm);
            log.info(query);
            QueryExecution qe = QueryExecutionFactory.create(query, model);
            try {
                ResultSet rs = qe.execSelect();
                while(rs.hasNext()) {
                    QuerySolution soln = rs.next();
                    Resource res = soln.getResource("person");
                    if(res != null) {
                        relevantResources.add(res);
                    }
                }
            } finally {
                if(qe != null) {
                    qe.close();
                }
            }
        }
        return relevantResources;
    }
    
    protected Model transformToRDF(String records) {
        Model m = xmlToRdf.toRDF(records);
        m = rdfUtils.renameBNodes(m, NAMESPACE_ETC, m);
        m = constructForVIVO(m);
        m = rdfUtils.smushResources(m, m.getProperty(
                PRODINRA_TBOX_NS + "identifier"));
        return m;
    }
    
    protected Model constructForVIVO(Model m) {
        // TODO dynamically get/sort list from classpath resource directory
        List<String> queries = Arrays.asList("100-documentTypes.sparql",
                "105-title.sparql",
                "102-authorshipPersonTypes.sparql",
                "107-authorLabel.sparql",
                "110-abstract.sparql",
                "112-keywords.sparql",
                "120-externalAffiliation.sparql",
                "122-inraAffiliationUnit.sparql",
                "124-inraLab.sparql");
        for(String query : queries) {
            log.info("Executing query " + query);
            log.info("Pre-query model size: " + m.size());
            construct(SPARQL_RESOURCE_DIR + query, m, NAMESPACE_ETC);
            log.info("Post-query model size: " + m.size());
        }
        return m;
    }
    
    public String listRecords() throws IOException {
        String url = ENDPOINT + "?verb=ListRecords&metadataPrefix=" 
                + METADATA_PREFIX;
        return httpUtils.getHttpResponse(url);
    }

    public Model getResult() {
        return this.result;
    }
   
}
