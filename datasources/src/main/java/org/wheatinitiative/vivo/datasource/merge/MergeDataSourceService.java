package org.wheatinitiative.vivo.datasource.merge;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;

@WebServlet(name = "MergeDataSourceService", urlPatterns = {"/dataSource/merge/*"} )
public class MergeDataSourceService extends DataSourceService {

    private static volatile DataSource dataSource = null;
    
    @Override
    protected DataSource getDataSource(HttpServletRequest request) {
        return getDataSourceInstance();
    }
    
    public static synchronized DataSource getDataSourceInstance() {
        if(dataSource == null) {
            dataSource = new MergeDataSource();
        }
        return dataSource;
    }
    
}
