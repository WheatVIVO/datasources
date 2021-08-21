package org.wheatinitiative.vivo.datasource.connector.orcid;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.SparqlEndpointParams;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class OrcidConnector extends ConnectorDataSource implements DataSource {

    private static final Log log = LogFactory.getLog(OrcidConnector.class);
    private static final String OAUTH_TOKEN_URL = 
            "https://orcid.org/oauth/token";
    private static final String PUBLIC_API_BASE_URL = 
            "https://pub.orcid.org/v2.0/";
    private static final String WHEAT_INITIATIVE = 
            "http://vivo.wheatinitiative.org/individual/";
    private static final String NAMESPACE_ETC = 
            WHEAT_INITIATIVE + "orcid-";
    private static final String TEMP_NS = "http://example.org/tmp/";
    private static final String DATASOURCE_CONFIG_PROPERTY_PREFIX = "datasource.";
    private static final int MIN_REST = 65; // ms
    private static final String CLIENT_ID = "orcid.clientId";
    private static final String CLIENT_SECRET = "orcid.clientSecret";
    private String clientId;
    private String clientSecret;
    private static final String ORCIDID = "http://vivoweb.org/ontology/core#orcidId";
    private static final String SPARQL_RESOURCE_DIR = "/orcid/sparql/";
    private static final String VCARD = "http://www.w3.org/2006/vcard/ns#";
    private static final String GIVEN_NAME = VCARD + "givenName";
    private static final String FAMILY_NAME = VCARD + "familyName";
    private HttpClient httpClient;
    private long lastRequestMillis = 0;
    private NameProcessor nameProcessor = new NameProcessor();
    
    public OrcidConnector() {
        Object clientId = this.getConfiguration().getParameterMap().get(CLIENT_ID);
        if(clientId instanceof String) {
            this.clientId = (String) clientId;
        }
        Object clientSecret = this.getConfiguration().getParameterMap().get(
                CLIENT_SECRET);
        if(clientSecret instanceof String) {
            this.clientId = (String) clientId;
        }
        if(this.clientId == null) {
            log.debug("Reading " + CLIENT_ID + " from Java system properties");
            this.clientId = System.getProperty(CLIENT_ID);
        }
        if(this.clientSecret == null) {
            log.debug("Reading " + CLIENT_SECRET + " from Java system properties");
            this.clientSecret = System.getProperty(CLIENT_SECRET);
        }
        httpClient = HttpClients.custom()
                .setRedirectStrategy(new LaxRedirectStrategy())
                .setUserAgent(HttpUtils.DEFAULT_USER_AGENT)
                .build();
    }
    
    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        return new OrcidModelIterator(getOrcidIds());
    }
    
    private class OrcidModelIterator implements IteratorWithSize<Model> {
        
        private List<String> orcidIdList;
        private Iterator<String> orcidIds;
        private String accessToken;
        private XmlToRdf xmlToRdf = new XmlToRdf();
        
        public OrcidModelIterator(List<String> orcidIds) {
            this.orcidIdList = orcidIds;
            this.orcidIds = orcidIdList.iterator();
            if(clientId == null) {
                log.info("Reading" + DATASOURCE_CONFIG_PROPERTY_PREFIX 
                        + CLIENT_ID + " from parameter map");
                clientId = getConfiguration().getParameterMap().get(
                        DATASOURCE_CONFIG_PROPERTY_PREFIX + CLIENT_ID).toString();
            }
            if(clientSecret == null) {
                log.info("Reading" + DATASOURCE_CONFIG_PROPERTY_PREFIX 
                        + CLIENT_SECRET + " from parameter map");
                clientSecret = getConfiguration().getParameterMap().get(
                        DATASOURCE_CONFIG_PROPERTY_PREFIX + CLIENT_SECRET).toString();
            }
            if(clientId == null || clientSecret == null) {
                throw new RuntimeException(
                        "ORCID client ID and client secret must be specified");
            }
            this.accessToken = getAccessToken(clientId, clientSecret);
        }

        public boolean hasNext() {
            return orcidIds.hasNext();
        }

        public Model next() {
            String orcidId = orcidIds.next();
            try {
                return getOrcidModel(orcidId);
            } catch (Exception e) {
                log.error("Error getting model for orcid iD " + orcidId, e);
                return ModelFactory.createDefaultModel();
            }
        }

        public Integer size() {
            if(orcidIdList == null) {
                return null;
            } else {
                return orcidIdList.size();
            }
        }
        
        private Model getOrcidModel(String orcidId) {
            if(orcidId == null || orcidId.length() < 36 || !orcidId.contains("orcid.org/")) {
                log.error("Skipping invalid orcid iD " + orcidId);
                return ModelFactory.createDefaultModel();
            }
            String orcidNum = orcidId.split("orcid.org/")[1];
            String orcidRecord = getOrcidResponse(PUBLIC_API_BASE_URL + orcidNum + "/record");
            Model record = xmlToRdf.toRDF(orcidRecord);
            String response = getOrcidResponse(PUBLIC_API_BASE_URL + orcidNum + "/works");
            log.debug("Response to request for ORCID record: \n" + response);
            Model workSummaries = xmlToRdf.toRDF(response);
            Model works = ModelFactory.createDefaultModel();
            StmtIterator workIt = workSummaries.listStatements(
                    null, workSummaries.getProperty(
                            XmlToRdf.GENERIC_NS + "put-code"), (RDFNode) null);
            while(workIt.hasNext()) {
                Statement stmt = workIt.next();
                if(stmt.getObject().isLiteral()) {
                    String putCode = stmt.getObject().asLiteral().getLexicalForm();
                    String work = getOrcidResponse(                
                            PUBLIC_API_BASE_URL + orcidNum + "/work/" + putCode);
                    works.add(xmlToRdf.toRDF(work));
                }
            }
            works.add(record);
            return rdfUtils.renameBNodes(
                    works, NAMESPACE_ETC, works);
        }
            
        private String getOrcidResponse(String path) {
            try {
                long msToSleep = MIN_REST - 
                        (System.currentTimeMillis() - lastRequestMillis);
                if(msToSleep > 0) {
                    Thread.sleep(msToSleep);
                }                
                lastRequestMillis = System.currentTimeMillis();
                HttpGet get = new HttpGet(path);
                get.addHeader("Content-Type", "application/vdn.orcid+xml");
                get.addHeader("Authorization", "Bearer " + accessToken);                
                log.info("Retrieving " + get.getURI());
                HttpResponse resp = httpClient.execute(get);
                try {
                    InputStream in = resp.getEntity().getContent();
                    return IOUtils.toString(in, "UTF-8");                    
                } finally {
                    EntityUtils.consume(resp.getEntity());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        
        private String getAccessToken(String clientId, String clientSecret) {
            URI tokenURI;
            try {
                tokenURI = new URIBuilder(OAUTH_TOKEN_URL).build();
                HttpPost meth = new HttpPost(tokenURI);
                meth.addHeader("Accept", "application/json");
                List<NameValuePair> postParameters = new ArrayList<NameValuePair>();
                postParameters.add(new BasicNameValuePair("client_id", clientId));
                postParameters.add(new BasicNameValuePair("client_secret", clientSecret));
                postParameters.add(new BasicNameValuePair("grant_type", "client_credentials"));
                postParameters.add(new BasicNameValuePair("scope", "/read-public"));           
                meth.setEntity(new UrlEncodedFormEntity(postParameters));
                HttpResponse resp = httpClient.execute(meth);
                String accessToken = "";
                //String orcid = "";
                try {
                    int status = resp.getStatusLine().getStatusCode();
                    log.debug("Status: " + status);
                    InputStream in = resp.getEntity().getContent();
                    String payload = IOUtils.toString(in, "UTF-8");
                    if(status >= 300) {
                        log.error("Unexpected response from ORCID: " + payload);
                        throw new RuntimeException(payload);
                    }
                    //log.info("Response from ORCID: \n" + payload);
                    JSONObject json = new JSONObject(payload);
                    //orcid = json.get("orcid").toString();
                    accessToken = json.getString("access_token");
                    //log.info("The object type for the orcid is " + orcid.getClass().getSimpleName());
                    //log.info("The ORCID iD we're dealing with is " + orcid);
                    //log.info("The access token is " + accessToken);
                } finally {
                    EntityUtils.consume(resp.getEntity());
                }        
                return accessToken;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    protected List<String> getOrcidIds() {
        List<String> orcidIds = new ArrayList<String>();
        SparqlEndpoint sourceEndpoint = getSourceEndpoint();
        ResultSet rs = sourceEndpoint.getResultSet(
                "SELECT DISTINCT ?o WHERE { ?x <" + ORCIDID + "> ?o }");
        while(rs.hasNext()) {
            QuerySolution qsoln = rs.next();
            RDFNode n = qsoln.get("o");
            if(n.isURIResource()) {
                orcidIds.add(n.asResource().getURI());
            }
        }
        return orcidIds;
    }
    
    private SparqlEndpoint getSourceEndpoint() {
        String sourceServiceURI = this.getConfiguration().getServiceURI();
        if(!sourceServiceURI.endsWith("/")) {
            sourceServiceURI += "/";
        }
        SparqlEndpointParams params = new SparqlEndpointParams();
        params.setEndpointURI(sourceServiceURI + "api/sparqlQuery");
        params.setEndpointUpdateURI(sourceServiceURI + "api/sparqlUpdate");
        params.setUsername(
                this.getConfiguration().getEndpointParameters().getUsername());
        params.setPassword(
                this.getConfiguration().getEndpointParameters().getPassword());
        return new SparqlEndpoint(params);
    }

    @Override
    protected Model filter(Model model) {
        Model filtered = ModelFactory.createDefaultModel();
        StmtIterator sit = model.listStatements();
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            if(!stmt.getPredicate().getURI().contains("orcid.org") 
                    && !stmt.getPredicate().getURI().contains(XmlToRdf.GENERIC_NS)
                    && !stmt.getPredicate().getURI().contains(XmlToRdf.VITRO_NS)
                    && !stmt.getPredicate().getURI().contains(TEMP_NS)) {
                filtered.add(stmt);
            }
        }
        return filtered;
    }

    @Override
    protected Model mapToVIVO(Model model) {
        model = renameByIdentifier(model, model.getProperty(
                XmlToRdf.GENERIC_NS + "put-code"), WHEAT_INITIATIVE, "orcid-work-");
        List<String> queries = Arrays.asList("100-documentTypes.sparql",
                "102-authorship.sparql",
                "103-knownPerson.sparql", 
                "107-tempPropsForNaming.sparql");
        executeQueries(queries, model);
        model = renameByIdentifier(model, model.getProperty(
                TEMP_NS + "localName"), WHEAT_INITIATIVE, "");
        executeQueries(Arrays.asList("104-personVcard.sparql"), model);
        parseNames(model);
        queries = Arrays.asList(
                "1041-personLabel.sparql",
                "1045-knownPersonRankMatch.sparql",
                "105-title.sparql",
                "113-year.sparql",
                "114-doi.sparql",
                "115-journal.sparql",
                "116-url.sparql",
                "119-internalSameAs.sparql",
                "129-sameRank.sparql"
                );
        executeQueries(queries, model);
        return rdfUtils.renameBNodes(
                model, NAMESPACE_ETC, model);
    }
    
    private void executeQueries(List<String> queries, Model model) {
        for(String query : queries) {
            log.debug("Executing query " + query);
            log.debug("Pre-query model size: " + model.size());
            construct(SPARQL_RESOURCE_DIR + query, model, NAMESPACE_ETC);
            log.debug("Post-query model size: " + model.size());
        }
    }
    
    private Model parseNames(Model model) {
        Model additions = ModelFactory.createDefaultModel();
        Model subtractions = ModelFactory.createDefaultModel();
        StmtIterator sit = model.listStatements(null, model.getProperty(FAMILY_NAME), (Literal) null);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            Name name = nameProcessor.parseName(stmt.getObject().asLiteral().getLexicalForm());
            additions.add(stmt.getSubject(), additions.getProperty(FAMILY_NAME), name.getFamilyName());
            if(name.getGivenName() != null) {
                additions.add(stmt.getSubject(), additions.getProperty(GIVEN_NAME), name.getGivenName());
            }
            subtractions.add(stmt);
        }
        subtractions.add(model.listStatements(null, model.getProperty(GIVEN_NAME), (Literal) null));
        model.remove(subtractions);
        model.add(additions);
        return model;
    }

    @Override
    protected String getPrefixName() {
        return "orcid";
    }

}
