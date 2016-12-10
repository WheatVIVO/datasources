package org.wheatinitiative.vivo.datasource.service;

import java.io.IOException;
import java.io.Writer;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.DataSourceDescription;
import org.wheatinitiative.vivo.datasource.connector.impl.Rcuk;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DataSourceService extends HttpServlet {

    /**
     * Retrieve current state of data source
     */
    @Override
    public void doGet(HttpServletRequest request, 
            HttpServletResponse response) throws IOException {
        DataSource dataSource = getDataSource(request);
        DataSourceDescription description = new DataSourceDescription();
        description.setConfiguration(dataSource.getConfiguration());
        description.setStatus(dataSource.getStatus());
        ObjectMapper mapper = new ObjectMapper();
        Writer out = response.getWriter();
        try {
            mapper.writerWithDefaultPrettyPrinter().writeValue(
                    out, description);
        } finally {
            out.flush();
            out.close();
        }
    }

    /**
     * Invoke processing
     */
    @Override
    public void doPost(HttpServletRequest request, 
            HttpServletResponse response) {
        // TODO implement me
    }
    
    /**
     * Parse class name from request URI and retrieve appropriate DataSource 
     * instance
     * @return DataSource associated with this service
     */
    private DataSource getDataSource(HttpServletRequest request) {
        // TODO implement me
        return new Rcuk(null);
    }
    
}


