package org.wheatinitiative.vivo.datasource.connector.prodinra;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.DataSourceBase;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.rdf.model.Model;

public class Prodinra extends DataSourceBase implements DataSource {

    public static final Log log = LogFactory.getLog(Prodinra.class);
    
    private static final String ENDPOINT = "http://oai.prodinra.inra.fr/ft";
    private static final String METADATA_PREFIX = "oai_inra";
    private static final String PRODINRA_TBOX_NS = "http://record.prodinra.inra.fr";
    private static final String PRODINRA_ABOX_NS = PRODINRA_TBOX_NS + "/individual/";
    private static final String NAMESPACE_ETC = PRODINRA_ABOX_NS + "n";
    private static final String SPARQL_RESOURCE_DIR = "/prodinra/sparql/";
    
    private List<String> filterTerms; 
    private Model result;
    
    private HttpUtils httpUtils = new HttpUtils();
    private XmlToRdf xmlToRdf = new XmlToRdf();
    
    public Prodinra(List<String> filterTerms) {
        this.filterTerms = filterTerms;
    }
    
    public void run() {
        try { 
            String records = listRecords();
            Model model = transformToRDF(records);
            model = filter(model);
        } catch (IOException e) {
            log.error(e, e);
        }
    }
    
    protected Model filter(Model model) {
        return model;
    }
    
    protected Model transformToRDF(String records) {
        Model m = xmlToRdf.toRDF(records);
        m = constructForVIVO(m);
        m = rdfUtils.renameBNodes(m, NAMESPACE_ETC, m);
        return m;
    }
    
    private Model constructForVIVO(Model m) {
        // TODO dynamically get/sort list from classpath resource directory
        List<String> queries = Arrays.asList("100-documentTypes.sparql",
                "105-title.sparql",
                "102-authorshipPersonTypes.sparql",
                "107-authorLabel.sparql",
                "110-abstract.sparql",
                "120-externalAffiliation.sparql",
                "122-inraAffiliationUnit.sparql");
        for(String query : queries) {
            construct(SPARQL_RESOURCE_DIR + query, m, NAMESPACE_ETC);
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
