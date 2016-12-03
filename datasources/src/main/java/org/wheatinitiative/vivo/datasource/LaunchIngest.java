package org.wheatinitiative.vivo.datasource;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.wheatinitiative.vivo.datasource.connector.Prodinra;
import org.wheatinitiative.vivo.datasource.connector.Rcuk;
import org.wheatinitiative.vivo.datasource.connector.Usda;
import org.wheatinitiative.vivo.datasource.connector.WheatInitiative;

import com.hp.hpl.jena.rdf.model.Model;

public class LaunchIngest {

    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Usage: LaunchIngest " 
                    + "rcuk|prodinra|usda|wheatinitiative " 
                    + "queryTerm ... [queryTermN]");
        } else {
            List<String> queryTerms = new LinkedList<String>(
                    Arrays.asList(args));
            queryTerms.remove(0);
            String connectorName = args[0];
            DataSource connector = null;
            if("rcuk".equals(connectorName)) {
                connector = new Rcuk(queryTerms);
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
                result.write(System.out, "N3");    
            }
        }
    }
    
}
