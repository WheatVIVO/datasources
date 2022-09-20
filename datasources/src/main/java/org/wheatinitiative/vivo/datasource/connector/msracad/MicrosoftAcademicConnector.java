package org.wheatinitiative.vivo.datasource.connector.msracad;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.connector.orcid.Name;
import org.wheatinitiative.vivo.datasource.connector.orcid.NameProcessor;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.json.JsonToXml;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;

public class MicrosoftAcademicConnector extends ConnectorDataSource implements DataSource {
    
    private final static String EVALUATE_API = 
            "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate";
    
    private final static int REQUEST_INTERVAL = 2000; // ms
    
    private final static String SPARQL_RESOURCE_DIR = "/msracad/sparql/";

    private static final List<String> FILTER_OUT = Arrays.asList(
            "generalizedXMLtoRDF/0.1", "vitro/0.7#position", "vitro/0.7#value",
            "XMLSchema-instance");
    private static final String FILTER_OUT_RES = "match_nothing";
    
    private final static int ENTITIES_PER_PAGE = 250;
    private HttpUtils httpUtils = new HttpUtils();
    private JsonToXml converter = new JsonToXml();
    private static final Log log = LogFactory.getLog(
            MicrosoftAcademicConnector.class);
    
    private String key1;
    
    public MicrosoftAcademicConnector() {
        this.key1 = System.getProperty("microsoftacademic.apikey");
	if(this.key1 == null) {
            log.error("Microsoft Academic API key must be specified in Java"
	            + " system property microsoftacademic.apikey");
	}
    }
    
    @Override
    public int getBatchSize() {
        return 1;
    }
    
    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        return new MsAcadIterator(this.getDefaultNamespace());
    }
    
    private class MsAcadIterator implements IteratorWithSize<Model> {

        private Map<String, String> orgs = new HashMap<String, String>();      
        private Iterator<String> orgIdIt = null;
        private int offset = 0;
        private boolean remainingEntities = true;
        private String currentOrg = "";
        private int requests = 0;
        private String defaultNamespace;
        
        public MsAcadIterator(String defaultNamespace) {
            this.defaultNamespace = defaultNamespace;
            orgs.put("placeholder", "placeholder");
            orgIdIt = orgs.values().iterator();
        }
        
        @Override
        public boolean hasNext() {
            return remainingEntities;
        }

        @Override
        public Model next() {
            try {
                if(offset == 0) {
                    if(!orgIdIt.hasNext()) {
                        throw new RuntimeException("Iterator exhausted");
                    } else {
                        currentOrg = orgIdIt.next();
                    }
                }
                Model m = null;
                try {
                    m = retrieveEntities(currentOrg, offset);
                    log.info(requests + " requests");
                } catch (Exception e) {
                    offset = offset + ENTITIES_PER_PAGE;
                    throw new RuntimeException(e);
                }
                if(m.contains(null, m.getProperty(
                        XmlToRdf.GENERIC_NS + "entities"), (RDFNode) null)) {
                    offset = offset + ENTITIES_PER_PAGE;                    
                    return m;
                } else {
                    if(!orgIdIt.hasNext()) {
                        remainingEntities = false;
                    } else {
                        if(offset == 0) {
                            log.warn("No entities found for org " + currentOrg);    
                        } else {
                            offset = 0;
                        }
                    }
                    return ModelFactory.createDefaultModel();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        
        private int iteration = 0;
        private long timeOfLastRequest = 0L;
        
        private Model retrieveEntities(String queryStr, int offset) 
                throws URISyntaxException {
            iteration++;
            log.info("retrieving entities for " + queryStr + " offset " + offset);            
            HttpClient httpClient = httpUtils.getHttpClient();
            URIBuilder builder = new URIBuilder(EVALUATE_API);
            
            //builder.setParameter("expr", "And(Y>1989, Ty='0', And(W='wheat', Or(Composite(F.FN='plant disease'), Composite(F.FN='agriculture'))))");
            builder.setParameter("expr", "And(Y>2015, Ty='0', W='wheat')");
            //builder.setParameter("expr", "And(Or(Ti='wheat', FN='wheat'), Composite(AA.AfId=" + queryStr + "))");
            builder.setParameter("model", "latest");
            builder.setParameter("count", Integer.toString(ENTITIES_PER_PAGE));
            builder.setParameter("offset", Integer.toString(offset));
            builder.setParameter("attributes", "Id,Ty,Ti,Y,D,CC,ECC,V,I,FP,LP,"
                    + "AA.AuN,AA.DAuN,AA.AfN,AA.AfId,AA.AuId,AA.S,F.FN,F.FId,"
                    + "J.JN,J.JId,C.CN,C.CId,RId,W,DN,FN,BV,S,DOI");

            URI uri = builder.build();
            HttpGet request = new HttpGet(uri);
            request.setHeader("Ocp-Apim-Subscription-Key", key1);

            try {
                long now = System.currentTimeMillis();
                long toWait = REQUEST_INTERVAL - (now - timeOfLastRequest);
                if(toWait > 0) {
                    try {
                        Thread.sleep(toWait); 
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                log.info("Requesting " + uri.toString());
                requests++;
                HttpResponse response = httpClient.execute(request);
                this.timeOfLastRequest = System.currentTimeMillis();
                log.debug("response " + response.getStatusLine().getStatusCode());
                try {
                    String json = EntityUtils.toString(response.getEntity()); 
                    log.debug(json);
                    log.debug("entity size " + json.length());
                    if(response.getStatusLine().getStatusCode() > 200) {
                        log.error(json);
                        throw new RuntimeException(json);
                    }
                    JSONObject jsonObj = new JSONObject(json);                    
                    JSONArray entities = jsonObj.getJSONArray("entities");
                    for(int i = 0; i < entities.length(); i++) {
                        JSONObject entity = entities.getJSONObject(i);
                        // these objects generate numeric tags that 
                        // interfere with the XML to RDF conversion
                        entity.remove("IA");
                        entity.remove("PR");
                        entity.remove("CC");
                        entity.remove("CitCon");
                        entity = removeNumericKeys(jsonObj);
                    }                    
                    log.debug(jsonObj.toString(2));
                    json = jsonObj.toString();
                    String xml = converter.convertJsonToXml(json);
                    log.debug(xml);
                    Model rdf = (new XmlToRdf()).toRDF(xml);
                    rdf = (new RdfUtils()).renameBNodes(
                            rdf, defaultNamespace + getPrefixName() + "-" + iteration + "-n", rdf);
                    rdf = splitNames(rdf);
                    return rdf;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    EntityUtils.consume(response.getEntity());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } 
        }
        
        private Model splitNames(Model rdf) {
            NameProcessor nameProc = new NameProcessor();
            List<Statement> additions = new ArrayList<Statement>();
            StmtIterator nameIt = rdf.listStatements(null, rdf.getProperty(XmlToRdf.GENERIC_NS + "DAuN"), (RDFNode) null);
            while(nameIt.hasNext()) {
                Statement nameStmt = nameIt.next();
                if(nameStmt.getObject().isLiteral()) {
                    Name name = nameProc.parseName(nameStmt.getObject().asLiteral().getLexicalForm());
                    additions.add(ResourceFactory.createStatement(
                            nameStmt.getSubject(), rdf.getProperty(
                                    XmlToRdf.GENERIC_NS + "firstName"), 
                            ResourceFactory.createPlainLiteral(name.getGivenName())));
                    additions.add(ResourceFactory.createStatement(
                            nameStmt.getSubject(), rdf.getProperty(
                                    XmlToRdf.GENERIC_NS + "lastName"), 
                            ResourceFactory.createPlainLiteral(name.getFamilyName())));
                }
            }
            rdf.add(additions);
            return rdf;
        }
        
        private JSONObject removeNumericKeys(JSONObject jsonObject) {
            // copy the list of keys to avoid any concurrent modification problems
            List<String> keys = new ArrayList<String>(jsonObject.keySet());
            for(String key : keys) {
                if(Character.isDigit(key.charAt(0))) {
                    jsonObject.remove(key);
                }
            }
            return jsonObject;
        }
        
        @Override
        public Integer size() {
            return 1;
        }

    }
    
    @Override
    protected Model filter(Model model) {
        Model filtered = ModelFactory.createDefaultModel();
        StmtIterator sit = model.listStatements();
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            if( (RDF.type.equals(stmt.getPredicate()))
                        && (stmt.getObject().isURIResource())
                        && (stmt.getObject().asResource().getURI().contains(FILTER_OUT.get(0))) ) {                                   
                continue;     
            } 
            boolean filterPredicate = false;
            for (String filterOut : FILTER_OUT) {
                if(stmt.getPredicate().getURI().contains(filterOut)) {
                    filterPredicate = true;
                    break;
                }
            }
            if(filterPredicate) {
                continue;
            }
            if(stmt.getSubject().isURIResource() 
                    && stmt.getSubject().getURI().contains(FILTER_OUT_RES)) {
                continue;
            }
            if(stmt.getObject().isURIResource() 
                    && stmt.getObject().asResource().getURI().contains(FILTER_OUT_RES)) {
                continue;
            }
            filtered.add(stmt);
        }
        return filtered;
    }

    @Override
    protected Model mapToVIVO(Model model) {
        model = renameByIdentifier(model, model.getProperty(
                XmlToRdf.GENERIC_NS + "Id"), getDefaultNamespace(), getPrefixName() + "-");
        model = capitalizeAffiliations(model);
        List<String> queries = Arrays.asList( 
                "100-article.sparql", 
                "200-author.sparql",
                "250-authorAffiliation.sparql",
                "300-journal.sparql",
                "400-subjectArea.sparql",
                "500-url.sparql"
                );
        for(String query : queries) {
            log.debug("Executing query " + query);
            long preSize = model.size();
            log.debug("Pre-query model size: " + preSize);
            construct(SPARQL_RESOURCE_DIR + query, model, getDefaultNamespace() + getPrefixName() + "-");
            long postSize = model.size();
            log.debug("Post-query model size: " + postSize);
            if(preSize == postSize) {
                log.info(query + " constructed no triples");
            }
        }
        return model;
    }
    
    private Model capitalizeAffiliations(Model model) {
        Model tmp = ModelFactory.createDefaultModel();
        tmp.add(model.listStatements(
                null, model.getProperty(XmlToRdf.GENERIC_NS + "AfN"), (RDFNode) null));
        StmtIterator sit = tmp.listStatements();
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            if(!stmt.getObject().isLiteral()) {
                log.warn("Found resource object for predicate g:AfN" );
                continue;
            }
            model.add(
                    stmt.getSubject(), stmt.getPredicate(), capitalizeAffiliation(
                            stmt.getObject().asLiteral().getLexicalForm()));
            model.remove(stmt);
        }
        return model;
    }
    
    private String capitalizeAffiliation(String uncapitalizedAffiliation) {
        StringBuilder out = new StringBuilder();
        String[] tokens = uncapitalizedAffiliation.split(" ");
        for(int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if("the".equals(token) || "and".equals(token) || "of".equals(token) || "for".equals(token)) {
                out.append(token);
            } else {
                out.append(StringUtils.capitalize(token));
            }
            if(i + 1 < tokens.length) {
                out.append(" ");
            }
        }
        return out.toString();    
    }

    @Override
    protected String getPrefixName() {
        return "msracad";
    }  
    
}
