package org.wheatinitiative.vivo.datasource.connector.wheatinitiative;

import org.wheatinitiative.vivo.datasource.connector.arc.ArcConnector;

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
