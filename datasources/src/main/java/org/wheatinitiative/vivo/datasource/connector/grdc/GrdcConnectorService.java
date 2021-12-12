package org.wheatinitiative.vivo.datasource.connector.grdc;

import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;

public class GrdcConnectorService extends DataSourceService {

    private static final long serialVersionUID = 1L;
    private static volatile DataSource dataSource = null;

    @Override
    protected DataSource getDataSource(HttpServletRequest request) {
        return getDataSourceInstance();
    }

    public static synchronized DataSource getDataSourceInstance() {
        if(dataSource == null) {
            dataSource = new GrdcConnector();
        }
        return dataSource;
    }

}
