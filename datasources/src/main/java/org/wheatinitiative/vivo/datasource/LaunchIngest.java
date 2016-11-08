package org.wheatinitiative.vivo.datasource;

import java.util.Arrays;
import java.util.List;

import org.wheatinitiative.vivo.datasource.connector.Rcuk;

public class LaunchIngest {

    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Usage: LaunchIngest <queryTerm> ... <queryTermN>");
        } else {
            List<String> queryTerms = Arrays.asList(args);
            Rcuk rcuk = new Rcuk(queryTerms);
            rcuk.run();
            rcuk.getResult().write(System.out, "N3");
        }
    }
    
}
