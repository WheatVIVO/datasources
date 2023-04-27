package org.wheatinitiative.vivo.datasource.connector.cordis;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;

@WebServlet(name = "CordisService", urlPatterns = {"/dataSource/cordis/*"} )
public class CordisService extends DataSourceService {

	private static final Cordis cordis = new Cordis(); 
	
	@Override
	protected DataSource getDataSource(HttpServletRequest request) {
		return cordis;
	}
	
	
}
