package edu.uci.ics.crawler4j.fetcher;

import edu.uci.ics.crawler4j.url.WebURL;

import java.util.concurrent.CompletionStage;

public interface PageFetcher {
    CompletionStage<PageFetchResult> fetchPage(WebURL webUrl);

    void shutDown();
}
