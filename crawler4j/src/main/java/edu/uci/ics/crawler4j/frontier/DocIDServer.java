package edu.uci.ics.crawler4j.frontier;

public interface DocIDServer {
    int getDocCount();

    int getDocId(String url);

    int getNewDocID(String url);

    void addUrlAndDocId(String url, int docId) throws Exception;

    boolean isSeenBefore(String url);

    void close();
}
