package org.wheatinitiative.vivo.datasource.connector.wheatinitiative;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.DataSourceBase;
import org.wheatinitiative.vivo.datasource.util.csv.CsvToRdf;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.vocabulary.XSD;

public class WheatInitiative extends DataSourceBase implements DataSource {

    private static final String SERVICE_URI = 
            "http://www.wheatinitiative.org/administration/users/csv";
    private static final String TBOX = 
            "http://www.wheatinitiative.org/administration/users/ontology/";
    private static final String ABOX = 
            "http://www.wheatinitiative.org/administration/users/";
    private static final String ABOX_ETC = ABOX + "n";
    private static final String VIVO_NS = "http://vivoweb.org/ontology/core#";
    private static final String SPARQL_RESOURCE_DIR = "/wheatinitiative/sparql/";
    private Model resultModel;
    
    private static final Log log = LogFactory.getLog(WheatInitiative.class);
    
    @Override
    public void runIngest() {
        CsvToRdf csvParser = getWheatInitiativeCsvParser();
        HttpUtils httpUtils = new HttpUtils();
        this.getStatus().setRunning(true);
        try {
            RdfUtils rdfUtils = new RdfUtils();
            String csvString = httpUtils.getHttpResponse(SERVICE_URI);
            Model model = csvParser.toRDF(csvString);
            model = rdfUtils.renameBNodes(model, ABOX_ETC, model);
            model = constructForVIVO(model);
            this.resultModel = model;
            log.info(model.size() + " triples in result model");
            writeResultsToEndpoint(model);
            // TODO any filter stage needed?
        } catch (Exception e) {
            log.info(e, e);
            throw new RuntimeException(e);
        } finally {
            this.getStatus().setRunning(false);
        }
    }

    private CsvToRdf getWheatInitiativeCsvParser() {
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
    private Model constructForVIVO(Model m) {
        // TODO dynamically get/sort list from classpath resource directory
        // TODO add remaining rules
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
    
    public Model getResult() {
        return this.resultModel;
    }
    
}
