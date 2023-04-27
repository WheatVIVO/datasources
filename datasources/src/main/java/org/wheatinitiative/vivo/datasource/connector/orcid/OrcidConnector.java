package org.wheatinitiative.vivo.datasource.connector.orcid;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.wheatinitiative.vivo.datasource.VivoVocabulary;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.util.ResourceUtils;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

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
    public static final String CLIENT_ID = "orcid.clientId";
    public static final String CLIENT_SECRET = "orcid.clientSecret";
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
    private Map<String, String> orcidIds = new HashMap<String, String>();
    private Map<String, String> journals = new HashMap<String, String>();
    
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
        log.info("clientId = " + clientId);
        log.info("clientSecret = " + clientSecret);
        httpClient = HttpClients.custom()
                .setRedirectStrategy(new LaxRedirectStrategy())
                //.setUserAgent(HttpUtils.DEFAULT_USER_AGENT)
                .build();
    }
    
    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        getOrcidIds();
        getJournals();
        return new OrcidModelIterator();
    }
    
    private class OrcidModelIterator implements IteratorWithSize<Model> {
        
        private Iterator<String> orcidIdIt;
        private String accessToken;
        private XmlToRdf xmlToRdf = new XmlToRdf();
        
        public OrcidModelIterator() {
            this.orcidIdIt = orcidIds.keySet().iterator();
            if(clientId == null) {
                log.info("Reading " + DATASOURCE_CONFIG_PROPERTY_PREFIX
                        + CLIENT_ID + " from parameter map");
                clientId = getConfiguration().getParameterMap().get(
                        DATASOURCE_CONFIG_PROPERTY_PREFIX + CLIENT_ID).toString();
            }
            if(clientSecret == null) {
                log.info("Reading" +  DATASOURCE_CONFIG_PROPERTY_PREFIX
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
            return orcidIdIt.hasNext();
        }
        
        public Model next() {
            String orcidId = orcidIdIt.next();
            try {
                return getOrcidModel(orcidId);
            } catch (Exception e) {
                log.error("Error getting model for orcid iD " + orcidId, e);
                return ModelFactory.createDefaultModel();
            }
        }

        public Integer size() {
            if(orcidIds == null) {
                return null;
            } else {
                return orcidIds.size();
            }
        }
        
        private Model getOrcidModel(String orcidId) {
            if(orcidId == null || orcidId.length() < 36 || !orcidId.contains("orcid.org/")) {
                log.error("Skipping invalid orcid iD " + orcidId);
                return ModelFactory.createDefaultModel();
            }
            String orcidNum = orcidId.split("orcid.org/")[1];
            String orcidRecord = getOrcidResponse(PUBLIC_API_BASE_URL + orcidNum + "/record");
            Model record = null;
            try {
                record = xmlToRdf.toRDF(orcidRecord);
            } catch (Exception e) {
                log.error("response : " + orcidRecord);
            }
            String response = getOrcidResponse(PUBLIC_API_BASE_URL + orcidNum + "/works");
            log.info("Response to request for ORCID works: \n" + response);
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
    
    protected Map<String, String> getOrcidIds() {
        SparqlEndpoint sourceEndpoint = getSourceEndpoint();
        ResultSet rs = sourceEndpoint.getResultSet(
                "SELECT DISTINCT ?o ?x WHERE { \n"
              + "  GRAPH ?g { ?x <" + ORCIDID + "> ?o } \n"
	      + "  FILTER(REGEX(STR(?g), \"wheatinitiative\")"
	      + "    || REGEX(STR(?g), \"kb-2\"))"
	      + "} \n");
        while(rs.hasNext()) {
            QuerySolution qsoln = rs.next();
            RDFNode o = qsoln.get("o");
            RDFNode x = qsoln.get("x");
            if(o.isURIResource() && x.isURIResource()) {
                String orcid = o.asResource().getURI();
                String person = x.asResource().getURI();
                log.info("Found person " + person + " with orcid " + orcid);
                orcidIds.put(orcid, person);
            }
        }
        return orcidIds;
    }
    
    protected Map<String, String> getJournals() {
        SparqlEndpoint sourceEndpoint = getSourceEndpoint();
        ResultSet rs = sourceEndpoint.getResultSet(
                "SELECT DISTINCT ?journal ?issn WHERE { ?journal a <" + VivoVocabulary.BIBO + "Journal> . \n "
                        + "  ?journal <" + VivoVocabulary.BIBO + "issn> ?issn \n"
                        + "}");
        while(rs.hasNext()) {
            QuerySolution qsoln = rs.next();
            RDFNode jo = qsoln.get("journal");
            RDFNode is = qsoln.get("issn");
            if(jo.isURIResource() && is.isLiteral()) {
                String journal = jo.asResource().getURI();
                String issn = is.asLiteral().getLexicalForm();
                log.info("Found journal " + journal + " with ISSN " + issn);
                journals.put(issn, journal);
            }
        }
        return journals;
    }
    
    private SparqlEndpoint getSourceEndpoint() {
        String sourceServiceURI = this.getConfiguration().getServiceURI();
        if(sourceServiceURI == null) {
            return this.getSparqlEndpoint();
        }
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
                "101-defaultType.sparql",
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
                "114a-pmid.sparql",
                "115-journal.sparql",
                "116-url.sparql",
                "119-internalSameAs.sparql",
                "129-sameRank.sparql"
                );
        executeQueries(queries, model);
        model = rdfUtils.renameBNodes(
                model, NAMESPACE_ETC, model);
        return postProcess(model);
    }
    
    private Model postProcess(Model model) {
        model = renameByOrcid(model);
        model = mergeRemainingSameAs(model);
        model = renameByIssn(model);
        model = deleteExtraAuthorships(model);
        return model;
    }
    
    private Model renameByOrcid(Model model) {
        return renameByMap(model, orcidIds, VivoVocabulary.VIVO + "orcidId");
    }
    
    private Model renameByIssn(Model model) {
        return renameByMap(model, journals, VivoVocabulary.BIBO + "issn");
    }
    
    private Model renameByMap(Model model, Map<String, String> map, String propertyURI) {
        log.info("Renaming by " + propertyURI);
        Map<Resource, String> rename = new HashMap<Resource, String>();        
        StmtIterator sit = model.listStatements(null, model.getProperty(
                propertyURI), (RDFNode) null);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            if(!stmt.getObject().isAnon()) {
                String value = null;
                if(stmt.getObject().isURIResource()) {
                    value = stmt.getObject().asResource().getURI();
                } else {
                    value = stmt.getObject().asLiteral().getLexicalForm();
                }
                log.info("Looking for value " + value);
                String vivoURI = map.get(value);
                if(vivoURI != null) {
                    log.info("Mapping " + value + " to " + vivoURI);
                    rename.put(stmt.getSubject(), vivoURI);
                    StmtIterator sameAsIt = model.listStatements(null, OWL.sameAs, stmt.getSubject());
                    while(sameAsIt.hasNext()) {
                        Statement sameAsStmt = sameAsIt.next();
                        if(!sameAsStmt.getSubject().equals(stmt.getSubject())) {
                            rename.put(sameAsStmt.getSubject(), vivoURI);
                        }
                    }
                }
            }
        }
        for(Resource r : rename.keySet()) {
            model.removeAll(r, RDFS.label, (RDFNode) null);
            model.removeAll(r, VivoVocabulary.HAS_CONTACT, (RDFNode) null);
            removeOrphaned(VivoVocabulary.VCARD + "Individual", model);
            removeOrphaned(VivoVocabulary.VCARD + "Name", model);
            ResourceUtils.renameResource(r, rename.get(r));
        }
        return model;
    }
    
    private Model mergeRemainingSameAs(Model model) {
        Map<Resource, String> rename = new HashMap<Resource, String>();  
        String queryStr = "SELECT ?x ?y WHERE { \n"
                + "  ?x <" + OWL.sameAs.getURI() + "> ?y \n"
                + "  FILTER(?x != ?y) \n"
                + "  FILTER(STR(?y) < STR(?x)) \n"
                + "  FILTER NOT EXISTS { \n"
                + "    ?x <" + OWL.sameAs.getURI() + "> ?z \n"
                + "    FILTER(STR(?z) < STR(?y)) \n"
                + "  } \n"
                + "} \n";
        QueryExecution qe = QueryExecutionFactory.create(queryStr, model);
        try {
            ResultSet rs = qe.execSelect();
            while(rs.hasNext()) {
                QuerySolution qsoln = rs.next();
                if(qsoln.get("x").isURIResource() && qsoln.get("y").isURIResource()) {
                    rename.put(qsoln.getResource("x"), qsoln.getResource("y").getURI());
                }
             }
        } finally {
            if(qe != null) {
                qe.close();
            }
        }
        for(Resource r : rename.keySet()) {
            String toURI = rename.get(r);
            log.info("Mapping " + r.getURI() + " to " + toURI);
            model.removeAll(r, RDFS.label, (RDFNode) null);
            model.removeAll(r, VivoVocabulary.HAS_CONTACT, (RDFNode) null);
            removeOrphaned(VivoVocabulary.VCARD + "Individual", model);
            removeOrphaned(VivoVocabulary.VCARD + "Name", model);
            ResourceUtils.renameResource(r, toURI);
        }
        return model;
    }
    
    private Model deleteExtraAuthorships(Model model) {
        String query = "CONSTRUCT { ?authorship ?p ?o . ?oo ?pp ?authorship } \n"
                + "WHERE { \n " 
                + "  ?authorship a <" + VivoVocabulary.VIVO + "Authorship> . \n"
                + "  ?work <" + VivoVocabulary.VIVO + "relatedBy> ?authorship . \n"
                + "  ?authorship <" + VivoVocabulary.VIVO + "relates> ?person . \n"
                + "  FILTER(?work != ?person) \n"
                + "  FILTER EXISTS { \n"
                + "    ?authorship2 a <" + VivoVocabulary.VIVO + "Authorship> . \n"
                + "    ?work <" + VivoVocabulary.VIVO + "relatedBy> ?authorship2 . \n"
                + "    ?authorship2 <" + VivoVocabulary.VIVO + "relates> ?person . \n"
                + "    FILTER(STR(?authorship2) < STR(?authorship)) \n"
                + "    FILTER(?authorship2 != ?authorship) \n"
                + "  } \n"
                + "  ?authorship ?p ?o . \n"
                + "  ?oo ?pp ?authorship . \n"
                + "} \n";
        QueryExecution qe = QueryExecutionFactory.create(query, model);
        try {
            Model toDelete = qe.execConstruct();
            log.info("Deleting " + toDelete.size() + " extra authorship triples.");
            model.remove(toDelete);
            return model;
        } finally {
            if(qe != null) {
                qe.close();
            }
        }

    }
    
    /**
     * Remove resources of the specified type if the resource does not occur
     * as the object of any triple.
     * @param typeURI
     * @param model
     */
    private void removeOrphaned(String typeURI, Model model) {
        List<Resource> toRemove = new ArrayList<Resource>();
        StmtIterator sit = model.listStatements(null, RDF.type, model.getResource(typeURI));
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            Resource res = stmt.getSubject();
            if(!model.contains(null, null, res)) {
                toRemove.add(res);
            }                  
        }
        for(Resource res : toRemove) {
            model.removeAll(res, null, (RDFNode) null);
        }
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