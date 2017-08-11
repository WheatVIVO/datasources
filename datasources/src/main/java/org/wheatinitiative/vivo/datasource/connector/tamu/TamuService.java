package org.wheatinitiative.vivo.datasource.connector.tamu;

import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;



public class TamuService extends DataSourceService {

    private static volatile DataSource dataSource = null;
    
    
    @Override
    protected DataSource getDataSource(HttpServletRequest request) {
        return getDataSourceInstance();
    }
    
    
    public static synchronized DataSource getDataSourceInstance() {
        if(dataSource == null) {
            dataSource = new Tamu();
        }
        return dataSource;
    }
    
}
