package edu.uci.ics.crawler4j.frontier;

import edu.uci.ics.crawler4j.url.WebURL;

public interface DocServer {

    void setScheduled(String url);

    void close();

    int getDocId(String url);

    long getLastScheduled(String url);

    long getLastSeen(String url);

    int getOrCreateDocID(String url);

    long now();

    void seen(WebURL url, int statusCode);

    default boolean shouldProcess(WebURL url) {
        long lastSeen = getLastSeen(url.getURL());
        return lastSeen <= (now() - (3 * 60 * 60));
    }

    default boolean shouldSchedule(WebURL url) {
        long lastScheduled = getLastScheduled(url.getURL());
        return lastScheduled <= (getLastSeen(url.getURL()));
    }
}
