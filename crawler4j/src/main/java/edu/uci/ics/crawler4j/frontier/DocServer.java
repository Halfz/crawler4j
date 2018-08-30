package edu.uci.ics.crawler4j.frontier;

public interface DocServer {

    int getDocId(String url);

    long getLastScheduled(String url);

    long getLastSeen(String url);

    long now();

    int getOrCreateDocID(String url);

    void close();

    void seen(String url, int statusCode);

    void setScheduled(String url);
}
