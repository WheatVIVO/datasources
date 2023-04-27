package org.wheatinitiative.vivo.datasource.connector.wheatinitiative;

import java.util.Arrays;
import java.util.List;

import org.apache.jena.rdf.model.Model;

public class OrganizationsConnector extends WheatInitiative {

    private static final String SPARQL_RESOURCE_DIR = "/wheatinitiative/organizations/sparql/";
    public static final String EXCEL_SUBDIR = "/organizations";
    private static final String ABOX = 
            "https://wheatvivo.org/";
    
    @Override
    protected String getABoxNS() {
        return ABOX;
    }
    
    @Override
    protected String getExcelSubdirectory() {
        return EXCEL_SUBDIR;
    }
    
    @Override
    protected String getPrefixName() {
        return "organization";
    }
    
    /**
     * Run a series of SPARQL CONSTRUCTS to generate VIVO-compatible RDF
     * @param m containing RDF lifted directly from source
     * @return model with VIVO RDF added
     */
    @Override
    protected Model mapToVIVO(Model m) {
        construct(SPARQL_RESOURCE_DIR + "050-local-name.sparql", m, getABoxNS());
        m = renameByIdentifier(m, m.getProperty(
                getTBoxNS() + "localName"), getABoxNS(), "");
        List<String> queries = Arrays.asList(
                "100-organization-name.sparql",
                "110-organization-type.sparql",
                "150-organization-address.sparql",
                "153-organization-phone.sparql",
                "156-organization-mail.sparql",
                "157-organization-alternative-email.sparql",
                "159-organization-webpage.sparql",
                "160-organization-webpage2.sparql"
                );
        for(String query : queries) {
            construct(SPARQL_RESOURCE_DIR + query, m, getABoxNS());
        }
        return m;
    }
    
}
