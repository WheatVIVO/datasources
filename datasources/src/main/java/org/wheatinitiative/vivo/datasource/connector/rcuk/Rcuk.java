package org.wheatinitiative.vivo.datasource.connector.rcuk;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.DataSourceBase;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.ResourceUtils;

public class Rcuk extends DataSourceBase implements DataSource {

    private static final Log log = LogFactory.getLog(Rcuk.class);
    private static final String API_URL = "http://gtr.rcuk.ac.uk/gtr/api/";
    private static final String RCUK_TBOX_NS = API_URL;
    private static final String RCUK_ABOX_NS = API_URL + "individual/";
    private static final Property HREF = ResourceFactory.createProperty(
            RCUK_TBOX_NS + "href");
    private static final Property LINK = ResourceFactory.createProperty(
            RCUK_TBOX_NS + "link");
    private static final Property TOTAL_PAGES = ResourceFactory.createProperty(
            RCUK_TBOX_NS + "totalPages");
    private static final String NAMESPACE_ETC = RCUK_ABOX_NS + "n";
    private static final String SPARQL_RESOURCE_DIR = "/rcuk/sparql/";
    private static final int MAX_SIZE = 100; // number of search results that can
                                             // be retrieved in a single request
    private static final int MAX_PAGES = 2;  // maximum number of pages to retrieve
                                             // for any search term
    private static final int MIN_REST_MILLIS = 350; // ms to wait between
                                                    // subsequent API calls
    
    private HttpUtils httpUtils = new HttpUtils();
    private XmlToRdf xmlToRdf = new XmlToRdf();
    //private List<String> queryTerms;
    private Model result;
    
    @Override
    public void runIngest() {
        // TODO progress percentage calculation from totals
        // TODO retrieving subsequent pages from API
        // TODO construct search terms with projects so we can take intersections?
        try {
            List<String> queryTerms = this.getConfiguration().getQueryTerms();
            Model m = ModelFactory.createDefaultModel();
            int totalRecords = 0;
            for(String queryTerm : queryTerms) {
                String projects = getProjects(queryTerm, 1);
                Model projectsRdf = transformToRdf(projects);
                m.add(projectsRdf);
                int totalPages = getTotalPages(projectsRdf);
                if (totalPages > MAX_PAGES) {
                    totalPages = MAX_PAGES;
                }
                if (totalPages > 1) {
                    for (int page = 2; page <= totalPages ; page++) {
                        m.add(transformToRdf(getProjects(queryTerm, page)));
                    }
                }
            }
            // TODO get total number of linked entities and use to update status
            m = addLinkedEntities(m);
            // get a certain subset of further entities related to these new entities
            m = addSecondLevelLinkedEntities(m);
            m = constructForVIVO(m);
            result = m;
            writeResultsToEndpoint(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            log.info("interrupted");
            // TODO any cleanup; running flag reset in finally block
        }
        log.info("done");
    }
    
    private int getTotalPages(Model projectsRdf) {
        int totalPages = 1;
        NodeIterator nodeIt = projectsRdf.listObjectsOfProperty(TOTAL_PAGES);
        while(nodeIt.hasNext()) { 
            // should be only once, but if not we'll just take the last value
            RDFNode node = nodeIt.next();
            if(node.isLiteral()) {
                try {
                    totalPages = node.asLiteral().getInt();
                } catch (Exception e) {
                    log.error("Invalid totalPages value " 
                            + node.asLiteral().getLexicalForm());
                }
            }
        }
        return totalPages;
    }
    
    @SuppressWarnings("unchecked")
    private List<String> getQueryTerms() {
        Object param = getConfiguration().getParameterMap().get("queryTerms");
        if(param instanceof List<?>) {
            @SuppressWarnings("rawtypes")
            List queryTerms = (List) param;
            if(queryTerms.isEmpty() || queryTerms.get(0) instanceof String) {
                return (List<String>) queryTerms;    
            }
        }
        return new ArrayList<String>();
    }
    
    public Model getResult() {
        return this.result;
    }
    
    private String getProjects(String queryTerm, int pageNum) 
            throws URISyntaxException {
        URIBuilder builder = new URIBuilder(API_URL + "projects");
        builder.addParameter("q", queryTerm);
        builder.addParameter("s", Integer.toString(MAX_SIZE, 10));
        builder.addParameter("p", Integer.toString(pageNum, 10));
        String url = builder.build().toString();
        try {
            log.info(url); // TODO level debug
            return httpUtils.getHttpResponse(url);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Run a series of SPARQL CONSTRUCTS to generate VIVO-compatible RDF
     * @param m containing RDF lifted directly from RCUK XML
     * @return model with VIVO RDF added
     */
    private Model constructForVIVO(Model m) {
        // TODO dynamically get/sort list from classpath resource directory
        List<String> queries = Arrays.asList("002-linkRelates.sparql", 
                "100-person-vcard-name.sparql", 
                "105-person-label.sparql",
                "200-organization-name.sparql",
                "210-organization-address.sparql",
                "300-grant.sparql",
                "310-grant-pi.sparql",
                "311-grant-copi.sparql",
                "320-grant-fund.sparql",
                "400-publication-title.sparql",
                "410-publication-JournalArticle.sparql",
                "411-publication-ConferenceProceedingAbstract.sparql",
                "412-publication-WorkingPaper.sparql",
                "413-publication-BookChapter.sparql",
                "414-publication-Thesis.sparql",
                "415-publication-Book.sparql",
                "430-publication-properties.sparql",
                "440-publication-supportedInformationResource.sparql",
                "455-position.sparql");
        for(String query : queries) {
            construct(SPARQL_RESOURCE_DIR + query, m, NAMESPACE_ETC);
        }
        return m;
    }
    
    /**
     * Traverse <link> nodes in the RCUK data and add RDF for the related 
     * entities
     * @param m model containing link nodes
     * @return m model with statements added describing linked entities
     */
    private Model addLinkedEntities(Model m) throws IOException, 
            InterruptedException {
        return addLinkedEntities(m, SPARQL_RESOURCE_DIR 
                + "getLinkedEntityURIs.sparql");
    }
    
    /**
     * Traverse <link> nodes in the RCUK data and add RDF for the related 
     * entities, using a more restricted SPARQL query to avoid pulling in
     * excessive unrelated nodes
     * @param m model containing link nodes
     * @return m model with statements added describing linked entities
     */
    private Model addSecondLevelLinkedEntities(Model m) throws IOException, 
            InterruptedException {
        return addLinkedEntities(m, SPARQL_RESOURCE_DIR 
                + "getLinkedEntityURIsLevel2.sparql");
    }
    
    /**
     * Traverse <link> nodes in the RCUK data and add RDF for the related 
     * entities
     * @param m model containing link nodes
     * @param queryName the name of the SPARQL query to load
     * @return m model with statements added describing linked entities
     */
    private Model addLinkedEntities(Model m, String queryName) throws IOException, 
            InterruptedException{
        String queryStr = loadQuery(queryName);
        QueryExecution qe = QueryExecutionFactory.create(queryStr, m);
        Set<String> linkedURIs = new HashSet<String>();
        try {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution soln = rs.next();
                Literal uri = soln.getLiteral("x");
                if(uri != null && uri.isLiteral()) {
                    linkedURIs.add(uri.getLexicalForm());
                }
            }
        } finally {
            qe.close();
        }
        for (String uri : linkedURIs) {
            log.info(uri); // TODO level debug
            String doc = httpUtils.getHttpResponse(uri);
            m.add(transformToRdf(doc));
            Thread.currentThread();
            Thread.sleep(MIN_REST_MILLIS);
        }
        return m;
    }
    
    private Model transformToRdf(String doc) {
        //InputStream inputStream = new ByteArrayInputStream(doc.getBytes());
        Model m = xmlToRdf.toRDF(doc);
        m = renameByHref(m);
        m = renameBlankNodes(m, NAMESPACE_ETC);
        // do a generic construct that will type each resource as an owl:Thing.
        m = construct(
                SPARQL_RESOURCE_DIR + "001-things.sparql", m, NAMESPACE_ETC);
        return m;
    }
       
    /**
     * Renames all blank nodes with URIs based on a namespaceEtc part 
     * concatenated with a random integer.
     * @param m model in which blank nodes are to be renamed
     * @param namespaceEtc the first (non-random) part of the generated URIs
     * @return model with named nodes
     */
    private Model renameBlankNodes(Model m, String namespaceEtc) {
        return rdfUtils.renameBNodes(m, namespaceEtc, m);
    }
    
    /**
     * Renames all blank nodes with URIs based on a namespaceEtc part 
     * concatenated with a random integer.
     * @param m model in which blank nodes are to be renamed
     * @param namespaceEtc the first (non-random) part of the generated URIs
     * @param dedupModel containing named resources whose URIs should not be reused
     * @return model with named nodes
     */
    private Model renameBlankNodes(Model m, String namespaceEtc, 
            Model dedupModel) {
        return rdfUtils.renameBNodes(m, namespaceEtc, m);
    }
    
    /**
     * Takes a model with blank nodes generated by lifting XML data into RDF
     * and renames certain resources based on the value of the href attribute.
     * This method will not rename 'link' resources because the href 
     * corresponds to the target of the link, which is assumed to be outside
     * the model.
     * @param m model lifted from XML containing blank nodes
     * @return model with some blank nodes possibly renamed with URIs
     */
    private Model renameByHref(Model m) {
        Map<Resource, String> bnodeToURI = new HashMap<Resource, String>();
        StmtIterator sit = m.listStatements(null, HREF, (Literal) null);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            Resource bnode = stmt.getSubject();
            if(!bnode.isAnon()) {
                continue; // unlikely, but let's check anyway
            }
            if(!m.contains(null, LINK, bnode)) {
                String uri = stmt.getObject().asLiteral().getLexicalForm();
                bnodeToURI.put(bnode, uri);
            }
        }
        for(Resource bnode : bnodeToURI.keySet()) {
            ResourceUtils.renameResource(bnode, bnodeToURI.get(bnode));
        }
        return m;
    }  
    
}
