package org.wheatinitiative.vivo.datasource.postmerge;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;

@WebServlet(name = "PostmergeDataSourceService", urlPatterns = {"/dataSource/postmerge/*"} )
public class PostmergeDataSourceService extends DataSourceService {

    private static final long serialVersionUID = 1L;
    private static volatile DataSource dataSource = null;
    
    @Override
    protected DataSource getDataSource(HttpServletRequest request) {
        return getDataSourceInstance();
    }
    
    public static synchronized DataSource getDataSourceInstance() {
        if(dataSource == null) {
            dataSource = new PostmergeDataSource();
        }
        return dataSource;
    }
    
}