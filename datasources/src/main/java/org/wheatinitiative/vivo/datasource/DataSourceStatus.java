package org.wheatinitiative.vivo.datasource;

public class DataSourceStatus {

    private boolean ok;
    private String message;
    private boolean isRunning;
    private int completionPercentage;
    private int totalRecords;
    private int processedRecords;
    private int errorRecords;

    public boolean isStatusOk() {
        return this.ok;
    }

    public String getMessage() {
        return this.message;
    }

    public boolean isRunning() {
        return this.isRunning;
    }

    public int getCompletionPercentage() {
        return this.completionPercentage;
    }

    public int getTotalRecords() {
        return this.totalRecords;
    }

    public int getProcessedRecords() {
        return this.processedRecords;
    }

    public int getErrorRecords() {
        return this.errorRecords;
    }     

    public void setStatusOk(boolean ok) {
        this.ok = ok;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setRunning(boolean running) {
        this.isRunning = running;
    }

    public void setCompletionPercentage(int completionPercentage) {
        this.completionPercentage = completionPercentage;
    }

    public void setTotalRecords(int totalRecords) {
        this.totalRecords = totalRecords;
    }

    public void setProcessedRecords(int processedRecords) {
        this.processedRecords = processedRecords;
    }

    public void setErrorRecords(int errorRecords) {
        this.errorRecords = errorRecords;
    }             

}