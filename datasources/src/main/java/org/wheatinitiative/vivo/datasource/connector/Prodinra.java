package org.wheatinitiative.vivo.datasource.connector;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.rdf.model.Model;

public class Prodinra implements DataSource {

    public static final Log log = LogFactory.getLog(Prodinra.class);
    
    private static final String ENDPOINT = "http://oai.prodinra.inra.fr/ft";
    private static final String METADATA_PREFIX = "oai_inra";
    private static final String PRODINRA_TBOX_NS = "http://record.prodinra.inra.fr";
    private static final String PRODINRA_ABOX_NS = PRODINRA_TBOX_NS + "/individual/";
    private static final String NAMESPACE_ETC = PRODINRA_ABOX_NS + "n";
    
    private List<String> filterTerms; 
    private Model result;
    
    private HttpUtils httpUtils = new HttpUtils();
    private XmlToRdf xmlToRdf = new XmlToRdf();
    private RdfUtils rdfUtils = new RdfUtils();
    
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
        m = rdfUtils.renameBNodes(m, NAMESPACE_ETC, m);
        return m;
    }
    
    public String listRecords() throws IOException {
        String url = ENDPOINT + "?verb=listRecords&metadataPrefix=" 
                + METADATA_PREFIX;
        return httpUtils.getHttpResponse(url);
    }

    public Model getResult() {
        return this.result;
    }
   
}
