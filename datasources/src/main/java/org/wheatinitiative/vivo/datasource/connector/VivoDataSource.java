package org.wheatinitiative.vivo.datasource.connector;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;
import org.wheatinitiative.vivo.datasource.connector.usda.Usda;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public abstract class VivoDataSource extends DataSourceBase {

    private static final String SEARCH_CONTROLLER = "/search";
    private static final String QUERYTEXT_PARAM = "querytext";
    private static final String XML_PARAM = "xml";
    private static final String XML_VALUE = "1";
    private static final String HITS_PER_PAGE_PARAM = "hitsPerPage";
    private static final String HITS_PER_PAGE = "100";
    private static final String CLASSGROUP_PARAM = "classgroup";
    
    private Log log = LogFactory.getLog(Usda.class);
    protected Model result;
    protected HttpUtils httpUtils = new HttpUtils();
    protected XmlToRdf xmlToRdf = new XmlToRdf();

    protected List<String> getUrisFromSearchResults(String vivoUrl, 
            String querytext) throws URISyntaxException, 
            IOException {
        return getUrisFromSearchResults(vivoUrl, querytext, null);
    }
    
    //TODO switch to iterator
    protected List<String> getUrisFromSearchResults(String vivoUrl, 
            String querytext, String classgroupURI) throws URISyntaxException, 
                IOException {
        List<String> resultUris = new ArrayList<String>();
        URIBuilder builder = new URIBuilder(vivoUrl + SEARCH_CONTROLLER);
        builder.addParameter(QUERYTEXT_PARAM, querytext);
        builder.addParameter(XML_PARAM, XML_VALUE);
        builder.addParameter(HITS_PER_PAGE_PARAM, HITS_PER_PAGE);
        if(classgroupURI != null) {
            builder.addParameter(CLASSGROUP_PARAM, classgroupURI);
        }
        URI nextPageUrl = builder.build();
        while(nextPageUrl != null) {
            System.out.println("Requesting " + nextPageUrl.toString());
            String searchResult = httpUtils.getHttpResponse(
                    nextPageUrl.toString());
            Model resultsModel = xmlToRdf.toRDF(searchResult);
            resultUris.addAll(getResultUris(resultsModel));
            String nextPage = getNextPageUrl(resultsModel);
            if(nextPage == null) {
                nextPageUrl = null;
            } else {
                // VIVO seems to have a bug where the classgroup filter 
                // isn't retained in the next page URL, so we
                // have to put it back in (yay!)
                URIBuilder nextPageBuilder = new URIBuilder(vivoUrl + nextPage);
                if(classgroupURI != null) {
                    nextPageBuilder.addParameter(CLASSGROUP_PARAM, 
                            classgroupURI);
                }
                nextPageUrl = nextPageBuilder.build();
            }
        }
        return resultUris;
    }
    
    private List<String> getResultUris(Model model) {
        return getValuesByName("uri", model);
    }
    
    private String getNextPageUrl(Model model) {
        List<String> nextPageValues = getValuesByName("nextPage", model);
        if(nextPageValues.isEmpty()) {
            return null;
        } else {
            return nextPageValues.get(0);
        }
    }
    
    private List<String> getValuesByName(String nameStr, Model model) {
        List<String> values = new ArrayList<String>();
        Property name = model.getProperty(XmlToRdf.GENERIC_NS + "name");
        Property value = model.getProperty(XmlToRdf.VITRO_NS + "value");
        ResIterator resIt = model.listSubjectsWithProperty(name, nameStr);
        while(resIt.hasNext()) { 
            Resource boxForValue = resIt.next();
            StmtIterator sit = model.listStatements(
                    boxForValue, value, (RDFNode) null);
            while(sit.hasNext()) {
                Statement stmt = sit.next();
                try {
                    values.add(stmt.getObject().asLiteral().getLexicalForm());
                } catch (Exception e) {
                    log.error("Invalid " + nameStr + " value " 
                            + stmt.getObject());
                }
            }
        }
        return values;
    }
    
    public Model getResult() {
        return this.result;
    }
    
}
