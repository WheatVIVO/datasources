package org.wheatinitiative.vivo.datasource.service;

import javax.servlet.annotation.WebServlet;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

@WebServlet(name = "VivoDataSourceService", urlPatterns = {"/dataSource/vivo/*"} )
public class VivoDataSourceService extends DataSourceServiceMultipleInstance {

    private static final long serialVersionUID = 1L;

    @Override
    protected DataSource constructDataSource() {
        return new VivoDataSource();
    }

}
