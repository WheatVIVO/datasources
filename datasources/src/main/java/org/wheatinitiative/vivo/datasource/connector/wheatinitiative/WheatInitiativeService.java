package org.wheatinitiative.vivo.datasource.connector.wheatinitiative;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;

@WebServlet(name = "WheatInitiativeService", urlPatterns = {"/dataSource/wheatinitiative/*"} )
public class WheatInitiativeService extends DataSourceService {

    private static volatile DataSource dataSource = null;
    
    @Override
    protected DataSource getDataSource(HttpServletRequest request) {
        return getDataSourceInstance();
    }
    
    public static synchronized DataSource getDataSourceInstance() {
        if(dataSource == null) {
            dataSource = new WheatInitiative();
        }
        return dataSource;
    }
    
}
