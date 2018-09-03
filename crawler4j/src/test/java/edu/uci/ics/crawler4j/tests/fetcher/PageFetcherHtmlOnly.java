package edu.uci.ics.crawler4j.tests.fetcher;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.fetcher.PageFetchResult;
import edu.uci.ics.crawler4j.fetcher.PageFetcherImpl;
import edu.uci.ics.crawler4j.url.WebURL;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpHead;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class PageFetcherHtmlOnly extends PageFetcherImpl {

    public PageFetcherHtmlOnly(CrawlConfig config) {
        super(config);
    }

    @Override
    public CompletionStage<PageFetchResult> fetchPage(WebURL webUrl) {
        String toFetchURL = webUrl.getURL();

        PageFetchResult fetchResult = new PageFetchResult();
        HttpHead head = null;
        try {
            head = new HttpHead(toFetchURL);

            synchronized (mutex) {
                long now = new Date().getTime();
                if (now - this.lastFetchTime < getConfig().getPolitenessDelay()) {
                    try {
                        Thread.sleep(getConfig().getPolitenessDelay() - (now - this.lastFetchTime));
                    } catch (InterruptedException e) {

                    }
                }
                this.lastFetchTime = new Date().getTime();
            }

            HttpResponse response = null;
            try {
                response = httpClient.execute(head);
            } catch (IOException e) {

            }
            fetchResult.setEntity(response.getEntity());
            fetchResult.setResponseHeaders(response.getAllHeaders());
            fetchResult.setFetchedUrl(toFetchURL);
            fetchResult.setStatusCode(response.getStatusLine().getStatusCode());

            String contentType = response.containsHeader("Content-Type") ?
                    response.getFirstHeader("Content-Type").getValue() : null;
            String typeStr = (contentType != null) ? contentType.toLowerCase() : "";

            if (typeStr.equals("") || (typeStr.contains("text") && typeStr.contains("html"))) {
                return super.fetchPage(webUrl);
            } else {
                return CompletableFuture.completedFuture(fetchResult);
            }
        } finally {
            if (head != null) {
                head.abort();
            }
        }
    }
}
