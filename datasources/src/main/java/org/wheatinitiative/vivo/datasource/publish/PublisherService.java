package org.wheatinitiative.vivo.datasource.publish;

import javax.servlet.annotation.WebServlet;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceServiceMultipleInstance;

@WebServlet(name = "PublisherService", urlPatterns = {"/dataSource/publish/*"} )
public class PublisherService extends DataSourceServiceMultipleInstance {

    private static final long serialVersionUID = 1L;

    @Override
    protected DataSource constructDataSource() {
        return new Publisher();
    }
    
}
