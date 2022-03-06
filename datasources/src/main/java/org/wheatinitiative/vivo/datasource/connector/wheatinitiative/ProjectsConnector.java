package org.wheatinitiative.vivo.datasource.connector.wheatinitiative;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.connector.arc.ArcConnector;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * A generic projects connector, using the same format as ARC
 *
 */
public class ProjectsConnector extends ArcConnector {

    public static final String EXCEL_SUBDIR = "/projects";
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
        return "project";
    }
    
}
