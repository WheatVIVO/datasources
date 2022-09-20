package org.wheatinitiative.vivo.datasource.connector.florida;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;

@WebServlet(name = "FloridaService", urlPatterns = {"/dataSource/florida/*"} )
public class FloridaService extends DataSourceService {
	
    private static volatile DataSource dataSource = null;
    
    
    @Override
    protected DataSource getDataSource(HttpServletRequest request) {
        return getDataSourceInstance();
    }
    
    
    public static synchronized DataSource getDataSourceInstance() {
        if(dataSource == null) {
            dataSource = new Florida();
        }
        return dataSource;
    }
    
}
