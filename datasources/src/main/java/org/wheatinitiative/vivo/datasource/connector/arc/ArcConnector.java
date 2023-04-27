package org.wheatinitiative.vivo.datasource.connector.arc;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.VivoVocabulary;
import org.wheatinitiative.vivo.datasource.connector.wheatinitiative.WheatInitiative;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;

public class ArcConnector extends WheatInitiative {

    public static final String EXCEL_SUBDIR = "/arc";
    private static final String TBOX = 
            "https://wheatvivo.org/ontology/arc/";
    private static final String ABOX = 
            "https://wheatvivo.org/";
    private static final String SPARQL_RESOURCE_DIR = "/arc/sparql/";
    private static final Log log = LogFactory.getLog(ArcConnector.class);
    
    public ArcConnector() {
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
    
    private Model splitInvestigators(Model m, String propertyURI) {
        Model investigators = ModelFactory.createDefaultModel();
        Property hasInvestigator = investigators.getProperty(propertyURI + "Split");
        StmtIterator sit = m.listStatements(null, m.getProperty(propertyURI), (RDFNode) null);
        while(sit.hasNext()) {
            Statement stmt  = sit.next();
            if(!stmt.getObject().isLiteral()) {
                continue;
            }
            String value = stmt.getObject().asLiteral().getLexicalForm();
            String[] persons = value.trim().split(";");
            for(int p = 0; p < persons.length; p++) {
                String person = persons[p];
                String[] personParts = person.trim().split(" +");
                if(personParts.length != 3) {
                    log.warn("personParts length expected 3,"
                            + " actual " + personParts.length + "(" + person + ")");
                }
                Resource grant = stmt.getSubject();
                Resource investigator = investigators.getResource(grant.getURI() + "-" + hasInvestigator.getLocalName() + (p + 1));
                investigators.add(grant, hasInvestigator, investigator);
                investigators.add(investigator, investigators.getProperty(
                        VivoVocabulary.VIVO + "rank"), Integer.toString(p + 1), XSDDatatype.XSDinteger);
                // first token is title
                // second token gets ignored if Prof, Dr
                // title Asst gets changed to Prof/A
                // last name is a concatenation of remaining tokens
                if("Asst".equals(personParts[0])) {
                    personParts[0] = "A/Prof";
                }
                int firstNameStart = 1;
                int lastNameStart = 2;
                if("Dr".equals(personParts[1]) || "Prof".equals(personParts[1])) {
                    firstNameStart = 2;
                    lastNameStart = 3;
                }
                investigators.add(investigator, investigators.getProperty(
                        XmlToRdf.GENERIC_NS + "title"), personParts[0]);
                investigators.add(investigator, investigators.getProperty(
                        XmlToRdf.GENERIC_NS + "firstName"), personParts[firstNameStart]);
                String lastName = "";
                for(int ln = lastNameStart; ln < personParts.length; ln++) {
                    lastName += personParts[ln];
                    if(ln < personParts.length - 1) {
                        lastName += " ";
                    }
                }
                investigators.add(investigator, investigators.getProperty(
                        XmlToRdf.GENERIC_NS + "lastName"), lastName);
            }
            
        }
        return investigators;
    }
    
    @Override
    protected Model mapToVIVO(Model m) {
        construct(SPARQL_RESOURCE_DIR + "050-localName.rq", m, ABOX);
        m = renameByIdentifier(m, m.getProperty(
                TBOX + "localName"), ABOX, "");
        m.add(splitInvestigators(m, TBOX + "Other_Investigators"));
        m.add(splitInvestigators(m, TBOX + "Lead_Investigator"));
        List<String> queries = Arrays.asList( 
                "100-grant.rq", 
                "110-funder.rq",
                "120-adminOrg.rq",
                "130-dti.rq",
                "200-pi.rq",
                "210-investigator.rq"
                );
        for(String query : queries) {
            construct(SPARQL_RESOURCE_DIR + query, m, ABOX);
        }
        return m;
    }
    
    @Override
    protected String getPrefixName() {
        return "arc";
    }
}
