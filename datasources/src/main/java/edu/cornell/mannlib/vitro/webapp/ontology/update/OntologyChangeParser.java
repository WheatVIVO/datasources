/* $This file is distributed under the terms of the license in /doc/license.txt$ */

package edu.cornell.mannlib.vitro.webapp.ontology.update;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.cornell.mannlib.vitro.webapp.ontology.update.AtomicOntologyChange.AtomicChangeType;

/**
 * Performs parsing on Prompt output and provides change object list.
 * 
 * @author ass92
 *
 */

public class OntologyChangeParser {

    private final Log log = LogFactory.getLog(OntologyChangeParser.class);
                
    /**
     * @param args
     * @throws IOException 
     */
    
    @SuppressWarnings({ "unchecked", "null", "static-access" })
    public ArrayList<AtomicOntologyChange> parseFile(URI diffPath) throws IOException{
        
        AtomicOntologyChange changeObj;
        ArrayList<AtomicOntologyChange> changeObjects = new ArrayList<AtomicOntologyChange>();
        int countColumns = 0;
        String URI = null;
        String rename = null;
        String sourceURI = null;
        String destinationURI = null;
        StringTokenizer stArr = null; 
        FileReader in = new FileReader(new File(diffPath));
        
        Iterable<CSVRecord> records = CSVFormat.TDF.parse(in);
        Iterator<CSVRecord> rows = records.iterator();
        //CSVReader readFile = new SimpleReader();
        //readFile.setSeperator('\t');
        
        //List<String[]> rows = readFile.parse(in);
        
        int rowNum = 0;
        while(rows.hasNext()) {
            rowNum++;
            CSVRecord row = rows.next();
            if (row.size() != 5) {
                log.error("Invalid PromptDiff data at row " + (rowNum + 1) 
                       + ". Expected 5 columns; found " + row.size() );
            } else {
                changeObj = new AtomicOntologyChange();
                if (row.get(0) != null && row.get(0).length() > 0) {
                    changeObj.setSourceURI(row.get(0));
                }
                if (row.get(1) != null && row.get(1).length() > 0) {
                    changeObj.setDestinationURI(row.get(1));
                }
                if (row.get(4) != null && row.get(4).length() > 0) {
                  changeObj.setNotes(row.get(4));
                }
                if ("Yes".equals(row.get(2))) {
                    changeObj.setAtomicChangeType(AtomicChangeType.RENAME);
                } else if ("Delete".equals(row.get(3))) {
                    changeObj.setAtomicChangeType(AtomicChangeType.DELETE); 
                } else if ("Add".equals(row.get(3))) {
                    changeObj.setAtomicChangeType(AtomicChangeType.ADD);
                } else {
                    log.error("Invalid rename or change type data: '" +
                            row.get(2) + " " + row.get(3) + "'");
                }
                log.debug(changeObj);
                changeObjects.add(changeObj);
            }
            
        }
        if (changeObjects.size() == 0) {
            log.debug("No ABox updates are required.");
        }
        return changeObjects;
    }

}

