package org.wheatinitiative.vivo.datasource.connector.rcuk;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;

@WebServlet(name = "RcukService", urlPatterns = {"/dataSource/rcuk/*"} )
public class RcukService extends DataSourceService {

    private static final Log log = LogFactory.getLog(RcukService.class);
    private static volatile DataSource dataSource = null;
    
    @Override
    protected DataSource getDataSource(HttpServletRequest request) {
        log.info("constructing");
        return getDataSourceInstance();
    }
    
    public static synchronized DataSource getDataSourceInstance() {
        if(dataSource == null) {
            dataSource = new Rcuk();
        }
        return dataSource;
    }
    
}
