package org.wheatinitiative.vivo.datasource.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.DataSourceDescription;

public abstract class DataSourceService extends HttpServlet {
    
    private static final long serialVersionUID = 1L;

    private static final Log log = LogFactory.getLog(DataSourceService.class);
    
    protected volatile WeakReference<Thread> workerThread;
    
    /**
     * Retrieve current state of data source
     */
    @Override
    public void doGet(HttpServletRequest request, 
            HttpServletResponse response) throws IOException {
        DataSourceDescriptionSerializer serializer = 
                new DataSourceDescriptionSerializer();
        DataSource dataSource = getDataSource(request);
        DataSourceDescription description = new DataSourceDescription(
                dataSource.getConfiguration(), dataSource.getStatus());
        OutputStream out = response.getOutputStream();
        try {
            serializer.serialize(description, out);
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
            HttpServletResponse response) throws IOException {
        DataSourceDescriptionSerializer serializer = 
                new DataSourceDescriptionSerializer();
        InputStream in = request.getInputStream();
        DataSourceDescription description = serializer.unserialize(in);
        DataSource dataSource = getDataSource(request);
        dataSource.setConfiguration(description.getConfiguration());
        if(isStartRequested(description, dataSource)) {
            log.info("start requested");
            startWork(dataSource);
            waitABitForWorkToStart(dataSource);
        } else if (isStopRequested(description, dataSource)) {
            log.info("stop requested");
            stopWork(dataSource); 
            waitABitForWorkToTerminate(dataSource);
        } else {
            log.info("neither start nor stop requested");
        }
        doGet(request, response);
    }
    
    private static final boolean RUNNING = true;
    
    /**
     * Wait a bit to see if the work starts before sending the current
     * status back to the client 
     * @param dataSource
     */
    private void waitABitForWorkToStart(DataSource dataSource) {
        waitForStatus(dataSource, RUNNING);
    }
    
    /**
     * Wait a bit to see if the work terminates before sending the current
     * status back to the client 
     * @param dataSource
     */
    private void waitABitForWorkToTerminate(DataSource dataSource) {
        waitForStatus(dataSource, !RUNNING);
    }
    
    private void waitForStatus(DataSource dataSource, boolean start) {
        int remainingSleeps = 5; // * 100 ms
        while (remainingSleeps > 0 && (dataSource.getStatus().isRunning() ^ start)) {
            remainingSleeps--;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
        }
    }
    
    protected boolean isStartRequested(DataSourceDescription description, 
            DataSource dataSource) {
        if(description == null || description.getStatus() == null) {
            return false;
        } else {
            boolean runningRequested = description.getStatus().isRunning();
            if(dataSource.getStatus() == null) {
                return runningRequested;
            } else {
                return runningRequested && !dataSource.getStatus().isRunning();
            }
        }
    }
    
    protected boolean isStopRequested(DataSourceDescription description, 
            DataSource dataSource) {
        if(description == null || description.getStatus() == null) {
            return false;
        } else {
            boolean stoppingRequested = !description.getStatus().isRunning();
            if(dataSource.getStatus() == null) {
                return stoppingRequested;
            } else {
                return stoppingRequested && dataSource.getStatus().isRunning();
            }
        }
    }
    
    protected void startWork(DataSource dataSource) {
        if(workerThread != null && workerThread.get() != null  
                && workerThread.get().isAlive()) {
            log.info("Thread already running");
            log.info("Thread interrupted? " + workerThread.get().isInterrupted());
            // already running
        } else {
            log.info("starting thread");
            Thread t = new Thread(dataSource);
            workerThread = new WeakReference<Thread>(t);
            t.start();
        }
    }
    
    protected void stopWork(DataSource dataSource) {
        dataSource.getStatus().setStopRequested(true);
        // interrupts are problematic because e.g. Riot catches them and
        // rethrows a different exception
//        if(workerThread == null || workerThread.get() == null 
//                && workerThread.get().isAlive()) {
//            // already stopped
//        } else {
//            try {
//                workerThread.get().interrupt();
//            } catch (NullPointerException e) {
//                // nothing to do; thread terminated while we weren't looking.
//            }
//        }
    }
    
    /**
     * To be overridden by subclasses.  Return a data source instance,
     * associated with the specific request if necessary
     */
    protected abstract DataSource getDataSource(HttpServletRequest request);
    
}


