package edu.uci.ics.crawler4j.frontier;

import edu.uci.ics.crawler4j.url.WebURL;

import java.util.List;


public interface Frontier {
    long getNumberOfAssignedPages();

    long getNumberOfProcessedPages();

    long getNumberOfScheduledPages();

    long getQueueLength();

    boolean isFinished();

    void setProcessed(WebURL webURL);

    void close();

    void finish();

    void getNextURLs(int max, List<WebURL> result);

    void schedule(WebURL url);

    void scheduleAll(List<WebURL> urls);
}
