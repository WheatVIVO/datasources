package org.wheatinitiative.vivo.datasource.connector.wheatinitiative;

import java.util.Arrays;
import java.util.List;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.CsvDataSource;
import org.wheatinitiative.vivo.datasource.util.csv.CsvToRdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.vocabulary.XSD;

public class WheatInitiative extends CsvDataSource implements DataSource {

    public static final String SERVICE_URI = 
            "http://www.wheatinitiative.org/administration/users/csv";
    private static final String TBOX = 
            "http://www.wheatinitiative.org/administration/users/ontology/";
    private static final String ABOX = 
            "http://www.wheatinitiative.org/administration/users/";
    private static final String ABOX_ETC = ABOX + "n";
    private static final String VIVO_NS = "http://vivoweb.org/ontology/core#";
    private static final String SPARQL_RESOURCE_DIR = "/wheatinitiative/sparql/";

    @Override
    protected CsvToRdf getCsvConverter() {
        CsvToRdf csvParser = new CsvToRdf();
        csvParser.addNullValueString("-");
        csvParser.addNullValueString("hidden");
        csvParser.addLiteralColumn(TBOX + "lastName");
        csvParser.addLiteralColumn(TBOX + "firstName");
        csvParser.addResourceColumn(VIVO_NS + "orcidId", "http://orcid.org/");
        csvParser.addLanguageLiteralColumn(TBOX + "role", "en"); 
        csvParser.addLiteralColumn(TBOX + "institutionCompany");
        csvParser.addLanguageLiteralColumn(TBOX + "employmentSector", "en");
        csvParser.addLanguageLiteralColumn(TBOX + "mainEmploymentCategory", "en");
        csvParser.addLanguageLiteralColumn(TBOX + "researchDomain", "en");
        csvParser.getLastColumn().setSplitValuesRegex(",");
        csvParser.addLanguageLiteralColumn(TBOX + "researchTopic", "en");
        csvParser.getLastColumn().setSplitValuesRegex(",");
        csvParser.addLanguageLiteralColumn(TBOX + "researchActivity", "en");
        csvParser.addLanguageLiteralColumn(TBOX + "otherProfessionalActivities", "en");
        csvParser.addLiteralColumn(TBOX + "personalWebpage", XSD.anyURI.getURI());
        csvParser.addLiteralColumn(TBOX + "addressCountry");
        csvParser.addLiteralColumn(TBOX + "addressLocality");
        csvParser.addLiteralColumn(TBOX + "addressPostalCode");
        csvParser.addLiteralColumn(TBOX + "addressThoroughfare");
        csvParser.addLiteralColumn(TBOX + "mail");
        csvParser.addLiteralColumn(TBOX + "phoneNumber");
        return csvParser;
    }
    
    /**
     * Run a series of SPARQL CONSTRUCTS to generate VIVO-compatible RDF
     * @param m containing RDF lifted directly from RCUK XML
     * @return model with VIVO RDF added
     */
    protected Model mapToVIVO(Model m) {
        construct(SPARQL_RESOURCE_DIR + "090-identifier-orcid.sparql", m, ABOX);
        construct(SPARQL_RESOURCE_DIR + "091-identifier-name.sparql", m, ABOX);
        construct(SPARQL_RESOURCE_DIR + "092-identifier-names.sparql", m, ABOX);
        m = renameByIdentifier(m, m.getProperty(
                TBOX + "identifier"), ABOX, "");
        List<String> queries = Arrays.asList( 
                "100-person-vcard-name.sparql", 
                "105-person-label.sparql",
                "150-person-address.sparql",
                "153-person-phone.sparql",
                "156-person-mail.sparql",
                "159-person-webpage.sparql",
                "200-organization-name.sparql"
                );
        for(String query : queries) {
            construct(SPARQL_RESOURCE_DIR + query, m, ABOX);
        }
        return m;
    }

    @Override
    protected String getABoxNamespaceAndPrefix() {
        return ABOX_ETC;
    }

    @Override
    protected Model filter(Model model) {
        // nothing to do, for now
        return model;
    }
    
}