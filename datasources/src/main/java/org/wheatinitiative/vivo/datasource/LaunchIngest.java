package org.wheatinitiative.vivo.datasource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.wheatinitiative.vivo.datasource.connector.prodinra.Prodinra;
import org.wheatinitiative.vivo.datasource.connector.rcuk.Rcuk;
import org.wheatinitiative.vivo.datasource.connector.usda.Usda;
import org.wheatinitiative.vivo.datasource.connector.wheatinitiative.WheatInitiative;

import com.hp.hpl.jena.rdf.model.Model;

public class LaunchIngest {

    public static void main(String[] args) {
        if(args.length < 3) {
            System.out.println("Usage: LaunchIngest " 
                    + "rcuk|prodinra|usda|wheatinitiative outputfile" 
                    + "queryTerm ... [queryTermN]");
        } else {
            List<String> queryTerms = new LinkedList<String>(
                    Arrays.asList(args));
            queryTerms.remove(0);
            queryTerms.remove(0);
            String connectorName = args[0];
            String outputFileName = args[1];
            DataSource connector = null;
            if("rcuk".equals(connectorName)) {
                connector = new Rcuk();
                connector.getConfiguration().getParameterMap().put(
                        "queryTerms", queryTerms);
            } else if ("prodinra".equals(connectorName)) {
                connector = new Prodinra(queryTerms);
            } else if ("usda".equals(connectorName)) {
                connector = new Usda(queryTerms);
            } else if ("wheatinitiative".equals(connectorName)) {
                connector = new WheatInitiative();
            } else {
                throw new RuntimeException("Connector not found: " 
                        + connectorName);
            }
            connector.run();
            Model result = connector.getResult();
            if(result != null) {
                //result.write(System.out, "N3");
                File outputFile = new File(outputFileName);
                FileOutputStream fos;
                try {
                    fos = new FileOutputStream(outputFile);
                    result.write(fos, "N3");
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    
}
