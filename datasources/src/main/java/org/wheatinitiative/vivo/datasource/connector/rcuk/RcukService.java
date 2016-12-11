package org.wheatinitiative.vivo.datasource.connector.rcuk;

import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;

public class RcukService extends DataSourceService {

    private static volatile DataSource dataSource = null;
    
    protected DataSource getDataSource(HttpServletRequest request) {
        return getDataSourceInstance();
    }
    
    public static synchronized DataSource getDataSourceInstance() {
        if(dataSource == null) {
            dataSource = new Rcuk();
        }
        return dataSource;
    }
    
}
