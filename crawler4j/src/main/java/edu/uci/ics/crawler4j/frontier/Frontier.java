package edu.uci.ics.crawler4j.frontier;

import edu.uci.ics.crawler4j.url.WebURL;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public interface Frontier {

    long getQueueLength();

    boolean isFinished();

    CompletionStage<Void> setProcessed(WebURL webURL);

    void close();

    void finish();

    CompletionStage<List<WebURL>> getNextURLs(int max);

    CompletionStage<Void> schedule(WebURL url);

    CompletionStage<Void> scheduleAll(List<WebURL> urls);
}
