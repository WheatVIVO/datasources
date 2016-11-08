package org.wheatinitiative.vivo.datasource.connector;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.ResourceUtils;

public class Rcuk implements Runnable {

    private static final Log log = LogFactory.getLog(Rcuk.class);
    private static final String API_URL = "http://gtr.rcuk.ac.uk/gtr/api/";
    private static final String RCUK_TBOX_NS = API_URL;
    private static final String RCUK_ABOX_NS = API_URL + "individual/";
    private static final Property HREF = ResourceFactory.createProperty(
            RCUK_TBOX_NS + "href");
    private static final Property LINK = ResourceFactory.createProperty(
            RCUK_TBOX_NS + "link");
    private static final String NAMESPACE_ETC = RCUK_ABOX_NS + "n";
    private static final String SPARQL_RESOURCE_DIR = "/rcuk/sparql/";
    private static final int MIN_REST_MILLIS = 700; // ms to wait between
                                                    // subsequent API calls
    
    private HttpClient httpClient;
    private RdfUtils rdfUtils;
    private List<String> queryTerms;
    private Model result;
    
    public Rcuk(List<String> queryTerms) {
        this.httpClient = new DefaultHttpClient();
        this.rdfUtils = new RdfUtils();
        this.queryTerms = queryTerms;
    }
    
    public void run() {
        // TODO progress percentage calculation from totals
        // TODO retrieving subsequent pages from API
        // TODO construct search terms with projects so we can take intersections?
        try {
            List<String> projects = getProjects(queryTerms);
            Model m = ModelFactory.createDefaultModel();
            for (String project : projects) {
                m.add(transformToRdf(project));
            }
            // TODO get total number of linked entities and use to update status
            m = addLinkedEntities(m);
            m = constructForVIVO(m);
            result = m;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e); // for now
        }
    }
    
    public List<String> getQueryTerms() {
        return this.queryTerms;
    }
    
    public Model getResult() {
        return this.result;
    }
    
    private List<String> getProjects(List<String> queryTerms) {
        List<String> projects = new ArrayList<String>();
        for (String queryTerm : queryTerms) {
            String url = API_URL + "projects?q=" + queryTerm;
            try {
                String response = getApiResponse(url);
                projects.add(response);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return projects;
    }
    
    /**
     * Run a series of SPARQL CONSTRUCTS to generate VIVO-compatible RDF
     * @param m containing RDF lifted directly from RCUK XML
     * @return model with VIVO RDF added
     */
    private Model constructForVIVO(Model m) {
        // TODO dynamically get/sort list from classpath resource directory
        List<String> queries = Arrays.asList("002-linkRelates.sparql");
        for(String query : queries) {
            construct(query, m);
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
            InterruptedException{
        String queryStr = loadQuery("getLinkedEntityURIs.sparql");
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
            String doc = getApiResponse(uri);
            m.add(transformToRdf(doc));
            Thread.currentThread();
            Thread.sleep(MIN_REST_MILLIS);
        }
        return m;
    }
    
    private Model transformToRdf(String doc) {
        InputStream inputStream = new ByteArrayInputStream(doc.getBytes());
        XmlToRdf xmlToRdf = new XmlToRdf();
        Model m = xmlToRdf.toRDF(inputStream);
        m = renameByHref(m);
        m = renameBlankNodes(m, NAMESPACE_ETC);
        // do a generic construct that will type each resource as an owl:Thing.
        m = construct("001-things.sparql", m);
        return m;
    }
    
    /**
     * Load a named CONSTRUCT query from a file in the SPARQL resources 
     * directory, run it against a model and add the results back to the model.
     * @param queryName name of SPARQL CONSTRUCT query to load
     * @param m model against which query should be executed
     * @return model with new data added by CONSTRUCT query
     */
    private Model construct(String queryName, Model m) {
        String queryStr = loadQuery(queryName);
        QueryExecution qe = QueryExecutionFactory.create(queryStr, m);
        try {
            Model tmp = ModelFactory.createDefaultModel();
            qe.execConstruct(tmp);
            m.add(tmp);
        } finally {
            if(qe != null) {
                qe.close();
            }
        }
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
    
    private String loadQuery(String resourceName) {
        resourceName = SPARQL_RESOURCE_DIR + resourceName;
        InputStream inputStream = this.getClass().getResourceAsStream(
                resourceName);
        StringBuffer fileContents = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String ln;
            while ( (ln = reader.readLine()) != null) {
                fileContents.append(ln).append('\n');
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to load " + resourceName, e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return fileContents.toString();
    }
    
    private String getApiResponse(String url) throws IOException {
        HttpGet get = new HttpGet(url);
        HttpResponse response = httpClient.execute(get);
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(
                    response.getEntity().getContent()));
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
        } finally {
            EntityUtils.consume(response.getEntity());
            if (br != null) {
                br.close();
            }
        }   
        return sb.toString();
    }
}
