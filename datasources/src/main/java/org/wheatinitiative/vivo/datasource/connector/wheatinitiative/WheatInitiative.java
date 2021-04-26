package org.wheatinitiative.vivo.datasource.connector.wheatinitiative;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class WheatInitiative extends ConnectorDataSource implements DataSource {
    
    public static final String SERVICE_URI = 
            "http://www.wheatinitiative.org/administration/users/csv";
    private static final String TBOX = 
            "http://www.wheatinitiative.org/administration/users/ontology/";
    private static final String ABOX = 
            "http://www.wheatinitiative.org/administration/users/";
    private static final String ABOX_ETC = ABOX + "n";
    private static final String VIVO_NS = "http://vivoweb.org/ontology/core#";
    private static final String SPARQL_RESOURCE_DIR = "/wheatinitiative/sparql/";
    private static final int MAX_COLS = 13;
    private static final Log log = LogFactory.getLog(WheatInitiative.class);

    @Override
    protected IteratorWithSize<Model> getSourceModelIterator() {
        Object dataDir = this.getConfiguration().getParameterMap().get("dataDir");
        if(!(dataDir instanceof String)) {
            throw new RuntimeException("dataDir parameter must point to " 
                    + "directory of researcher Excel files");
        }
        return new WheatInitiativeIterator((String) dataDir);
    }

    private class WheatInitiativeIterator implements IteratorWithSize<Model> {        

        List<File> files;                
        Iterator<File> fileIt;
        ArrayList<String> propertyURIs = new ArrayList<String>(); 

        public WheatInitiativeIterator(String dataDir) {
            log.info("Using dataDir " + dataDir);
            File pubs = new File(dataDir);            
            this.files = Arrays.asList(pubs.listFiles());
            fileIt = files.iterator();
        }

        @Override
        public boolean hasNext() {
            return fileIt.hasNext();
        }

        @Override
        public Model next() {
            Model model = ModelFactory.createDefaultModel();
            Workbook wb = null;
            try {
                File f = fileIt.next();
                log.info("Processing " + f.getName());
                String localNamePrefix = StringUtils.stripAccents(
                        f.getName().replaceAll("xlsx", "").trim()).replaceAll("\\W", "-");
                wb = WorkbookFactory.create(f);
                DataFormatter fmt = new DataFormatter();
                int sheetNum = 0;
                for(Sheet sheet : wb) {
                    int rowNum = 0;
                    if(wb.isSheetHidden(wb.getSheetIndex(sheet))) {
                        continue;
                    }
                    sheetNum++;                    
                    for (Row row : sheet) {
                        rowNum++;
                        int lastCellNum = row.getLastCellNum();
                        log.info("Last cell num is " + lastCellNum);
                        if(lastCellNum >= MAX_COLS) {
                            lastCellNum = MAX_COLS - 1;
                        }
                        if(rowNum == 1) {
                            // header row
                            log.info("Processing header.");
                            for(int i = row.getFirstCellNum(); i <= lastCellNum; i++) {                                
                                String header = fmt.formatCellValue(row.getCell(i));
                                String localName = localNameFromHeader(header);
                                String ontologyURI = TBOX;
                                propertyURIs.add(i, ontologyURI + localName);
                            }
                            continue;
                        } else {
                            log.info("Processing row " + rowNum);
                        }
                        String resourceURI = ABOX + localNamePrefix;
                        if(sheetNum > 1) {
                            resourceURI += "-s" + sheetNum;
                        }
                        resourceURI += "-researcher" + rowNum;
                        Resource res = model.getResource(resourceURI);
                        if(row.getFirstCellNum() < 0) {
                            break;
                        }                        
                        for(int i = row.getFirstCellNum(); i <= lastCellNum; i++) {
                            String propertyURI = propertyURIs.get(i);
                            Cell cell = row.getCell(i);
                            if(cell == null) {
                                continue;
                            } else if(cell.getHyperlink() != null) {
                                addProperty(res, propertyURI + "_url", 
                                        cell.getHyperlink().getAddress(), model);
                            }
                            String text;                            
                            if(CellType.FORMULA.equals(cell.getCellType())) {
                                text = cell.getCellFormula();
                            } else {
                                text = fmt.formatCellValue(row.getCell(i));
                            }
                            // sometimes Excel doesn't recognize the date fields
                            // TODO refactor this : also used in PublicationsConnector
                            if(propertyURI.contains("DATE") || propertyURI.contains("YEAR") 
                                    || propertyURI.contains("START") || propertyURI.contains("END")) {
                                java.text.SimpleDateFormat oldFmt = new java.text.SimpleDateFormat("dd/MM/yyyy");
                                java.text.SimpleDateFormat newFmt = new java.text.SimpleDateFormat("yyyy-MM-dd");
                                try {
                                    Date d = oldFmt.parse(text);
                                    text = newFmt.format(d);
                                } catch (ParseException e) {
                                    log.debug("Cannot parse " + text + " as date");
                                }
                            }
                            // non-breaking space
                            text = text.replaceAll("\u00A0", " ").trim();                                
                            if(text.isEmpty()) {
                                continue;
                            }
                            addProperty(res, propertyURI, text, model);
                        }
                    }
                }                
            } catch (EncryptedDocumentException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {                
                if(wb != null) {
                    try {
                        wb.close();
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
            }
            return model;
        }
        
        private String localNameFromHeader(String header) {
            header = header.replaceAll("\\#",  "Number");
            header = header.replaceAll("\\s+", "_").replaceAll("\\/", "-");
            // add other string munging as necessary
            return header;
        }
        
        private void addProperty(Resource res, String propertyURI, 
                String text, Model model) {                 
            Literal lit = ResourceFactory.createPlainLiteral(text);            
            model.add(res, model.getProperty(propertyURI), lit);   
            if(false) {
                    model.add(res, model.getProperty(propertyURI + "Ascii"), 
                            StringUtils.stripAccents(text.toLowerCase()));    
            }
        }                        

        @Override
        public Integer size() {
            return files.size();
        }

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
                "157-person-alternative-email.sparql",
                "159-person-webpage.sparql",
                "160-person-webpage2.sparql",
                "200-organization-name.sparql"
                );
        for(String query : queries) {
            construct(SPARQL_RESOURCE_DIR + query, m, ABOX);
        }
        return m;
    }

    @Override
    protected Model filter(Model model) {
        // nothing to do, for now
        return model;
    }

    @Override
    protected String getPrefixName() {
        return "wi";
    }
    
}