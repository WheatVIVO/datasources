package org.wheatinitiative.vivo.datasource.connector.grdc;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.connector.wheatinitiative.WheatInitiative;

import com.hp.hpl.jena.rdf.model.Model;

public class GrdcConnector extends WheatInitiative {

    public static final String EXCEL_SUBDIR = "/grdc";
    private static final String TBOX = 
            "https://wheatvivo.org/ontology/grdc/";
    private static final String ABOX = 
            "https://wheatvivo.org/";
    private static final String SPARQL_RESOURCE_DIR = "/grdc/sparql/";
    private static final Log log = LogFactory.getLog(GrdcConnector.class);

    public GrdcConnector() {
        propDateFormat.put("Funding_Commencement_Year", new java.text.SimpleDateFormat("MM/dd/yy"));
        propDateFormat.put("Anticipated_End_Date", new java.text.SimpleDateFormat("MM/dd/yy"));
    }
    
    @Override
    protected int getHeaderRow() {
        return 2;
    }

    @Override
    protected String getTBoxNS() {
        return TBOX;
    }

    @Override
    protected String getABoxNS() {
        return ABOX;
    }

    @Override
    protected String getExcelSubdirectory() {
        return EXCEL_SUBDIR;
    }

    @Override
    protected Model mapToVIVO(Model m) {
        construct(SPARQL_RESOURCE_DIR + "050-localName.rq", m, ABOX);
        m = renameByIdentifier(m, m.getProperty(
                TBOX + "localName"), ABOX, "");
        List<String> queries = Arrays.asList( 
                "100-grant.rq", 
                "110-funder.rq",
                "120-adminOrg.rq",
                "130-dti.rq"
                );
        for(String query : queries) {
            construct(SPARQL_RESOURCE_DIR + query, m, ABOX);
        }
        return m;
    }
    
    @Override
    protected String getPrefixName() {
        return "grdc";
    }

}
