package org.wheatinitiative.vivo.datasource.connector.wheatinitiative;

import java.io.IOException;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.DataSourceBase;
import org.wheatinitiative.vivo.datasource.util.csv.CsvToRdf;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.vocabulary.XSD;

public class WheatInitiative extends DataSourceBase implements DataSource {

    private static final String SERVICE_URI = 
            "http://www.wheatinitiative.org/administration/users/csv";
    private static final String TBOX = 
            "http://www.wheatinitiative.org/administration/users/ontology/";
    private static final String VIVO_NS = "http://vivoweb.org/ontology/core#";
    private Model resultModel;
    
    public void run() {
        CsvToRdf csvParser = getWheatInitiativeCsvParser();
        HttpUtils httpUtils = new HttpUtils();
        try {
            String csvString = httpUtils.getHttpResponse(SERVICE_URI);
            this.resultModel = csvParser.toRDF(csvString);
            // TODO any filter stage needed?
            // TODO map
        } catch (IOException e) {
            throw new RuntimeException(e);
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
    
    public Model getResult() {
        return this.resultModel;
    }
    
}
