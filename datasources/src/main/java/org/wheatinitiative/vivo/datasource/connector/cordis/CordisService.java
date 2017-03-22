package org.wheatinitiative.vivo.datasource.connector.cordis;

import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;

public class CordisService extends DataSourceService {

    private static final Cordis cordis = new Cordis(); 
    
    @Override
    protected DataSource getDataSource(HttpServletRequest request) {
        return cordis;
    }
    
    
}