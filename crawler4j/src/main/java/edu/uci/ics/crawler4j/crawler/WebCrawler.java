/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.crawler4j.crawler;

import edu.uci.ics.crawler4j.crawler.exceptions.ContentFetchException;
import edu.uci.ics.crawler4j.crawler.exceptions.PageBiggerThanMaxSizeException;
import edu.uci.ics.crawler4j.crawler.exceptions.ParseException;
import edu.uci.ics.crawler4j.fetcher.PageFetchResult;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.frontier.DocServer;
import edu.uci.ics.crawler4j.frontier.Frontier;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.parser.NotAllowedContentException;
import edu.uci.ics.crawler4j.parser.ParseData;
import edu.uci.ics.crawler4j.parser.Parser;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import edu.uci.ics.crawler4j.url.WebURL;
import org.apache.http.HttpStatus;
import org.apache.http.impl.EnglishReasonPhraseCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WebCrawler class in the Runnable class that is executed by each crawler thread.
 *
 * @author Yasser Ganjisaffar
 */
public class WebCrawler implements Runnable {

    protected static final Logger logger = LoggerFactory.getLogger(WebCrawler.class);

    /**
     * The id associated to the crawler thread running this instance
     */
    protected int myId;

    /**
     * The controller instance that has created this crawler thread. This
     * reference to the controller can be used for getting configurations of the
     * current crawl or adding new seeds during runtime.
     */
    protected CrawlController myController;
    /**
     * The thread within which this crawler instance is running.
     */
    private Thread myThread;
    /**
     * The parser that is used by this crawler instance to parse the content of the fetched pages.
     */
    private Parser parser;
    /**
     * The fetcher that is used by this crawler instance to fetch the content of pages from the web.
     */
    private PageFetcher pageFetcher;
    /**
     * The RobotstxtServer instance that is used by this crawler instance to
     * determine whether the crawler is allowed to crawl the content of each page.
     */
    private RobotstxtServer robotstxtServer;
    /**
     * The DocServer that is used by this crawler instance to map each URL to a unique docid.
     */
    private DocServer docServer;
    /**
     * The Frontier object that manages the crawl queue.
     */
    private Frontier frontier;
    /**
     * Is the current crawler instance waiting for new URLs? This field is
     * mainly used by the controller to detect whether all of the crawler
     * instances are waiting for new URLs and therefore there is no more work
     * and crawling can be stopped.
     */
    private AtomicBoolean isWaitingForNewURLs;


    @Override
    public void run() {
        AtomicReference<List<WebURL>> lastAssignedUrls = new AtomicReference<>(new ArrayList<>());
        try {
            onStart();
            while (!myController.isShuttingDown()) {
                isWaitingForNewURLs.set(true);
                frontier.getNextURLs(50).thenCompose(assignedURLs -> {
                    lastAssignedUrls.set(assignedURLs);
                    isWaitingForNewURLs.set(false);
                    if (assignedURLs.isEmpty()) {
                        if (frontier.isFinished()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            logger.error("Error occurred", e);
                        }
                        return CompletableFuture.completedFuture(null);
                    } else {
                        List<CompletableFuture<Void>> ret = new ArrayList<>();
                        for (WebURL curURL : assignedURLs) {
                            if (myController.isShuttingDown()) {
                                logger.info("Exiting because of controller shutdown.");
                                return CompletableFuture.completedFuture(null);
                            }
                            if (curURL != null) {
                                WebURL processUrl = handleUrlBeforeProcess(curURL);

                                if (shouldProcess(processUrl)) {
                                    ret.add(processPage(processUrl).thenAccept((Void) -> frontier.setProcessed(curURL)).toCompletableFuture());
                                } else {
                                    ret.add(frontier.setProcessed(curURL).toCompletableFuture());
                                }
                            }
                        }
                        return CompletableFuture.allOf(ret.toArray(new CompletableFuture[0]));
                    }
                }).exceptionally(throwable -> {
                    logger.error("crawler throws error", throwable);
                    return null;
                }).toCompletableFuture().get();
            }
        } catch (Exception e) {
            List<WebURL> webURLS = lastAssignedUrls.get();
            frontier.scheduleAll(webURLS);
            webURLS.forEach(frontier::setProcessed);
            logger.error("thread {} dead unexpectedly", myId, e);
            throw new RuntimeException(e);
        }
    }

    public CrawlController getMyController() {
        return myController;
    }

    /**
     * Get the id of the current crawler instance
     *
     * @return the id of the current crawler instance
     */
    public int getMyId() {
        return myId;
    }

    /**
     * The CrawlController instance that has created this crawler instance will
     * call this function just before terminating this crawler thread. Classes
     * that extend WebCrawler can override this function to pass their local
     * data to their controller. The controller then puts these local data in a
     * List that can then be used for processing the local data of crawlers (if needed).
     *
     * @return currently NULL
     */
    public Object getMyLocalData() {
        return null;
    }

    public Thread getThread() {
        return myThread;
    }

    public void setThread(Thread myThread) {
        this.myThread = myThread;
    }


    public boolean isNotWaitingForNewURLs() {
        return !isWaitingForNewURLs.get();
    }

    public CompletionStage<Page> fetchPage(WebURL curURL) {

        if (curURL == null) {
            logger.warn("processPage method received null url");
            return CompletableFuture.completedFuture(null);
        }


        curURL.setDocid(docServer.getOrCreateDocID(curURL.getURL()));

        return pageFetcher.fetchPage(curURL).thenApply((fetchResult) -> {

            Page page = new Page(curURL);
            try {
                int statusCode = fetchResult.getStatusCode();
                handlePageStatusCode(curURL, statusCode, EnglishReasonPhraseCatalog.INSTANCE.getReason(statusCode,
                        Locale.ENGLISH));
                // Finds the status reason for all known statuses

                page.setFetchResponseHeaders(fetchResult.getResponseHeaders());
                page.setStatusCode(statusCode);
                if (statusCode < 200 || statusCode > 299) { // Not 2XX: 2XX status codes indicate success
                    if (isRedirect(statusCode)) { // is 3xx  todo
                        processPageRedirectSync(page, curURL, fetchResult);
                    } else { // All other http codes other than 3xx & 200
                        processPageErrorSync(curURL, fetchResult);
                    }

                } else {
                    processPage200Sync(page, curURL, fetchResult);
                }
            } catch (ParseException pe) {
                onParseError(curURL);
            } catch (ContentFetchException cfe) {
                if (cfe.getCause() != null && cfe.getCause() instanceof SocketTimeoutException) {
                    logger.trace("socketTimeoutException", cfe.getCause());
                }
                onContentFetchError(page);
            } catch (NotAllowedContentException nace) {
                logger.debug("Skipping: {} as it contains binary content which you configured not to crawl",
                        curURL.getURL());
            } catch (Exception e) {
                onUnhandledException(curURL, e);
            } finally {
                if (fetchResult != null) {
                    fetchResult.discardContentIfNotConsumed();
                }
            }
            return page;
        }).exceptionally(e -> {
            if (e.getCause() instanceof PageBiggerThanMaxSizeException) {
                onPageBiggerThanMaxSize(curURL.getURL(),
                        ((PageBiggerThanMaxSizeException) (e.getCause())).getPageSize());
            } else {
                onUnhandledException(curURL, e);
            }
            // TODO 여기서 에러를 던지는게 맞아보인다.
            // TODO 에러를 던지면 상단에서 받아줘야함
            return null;
        });
    }

    public CompletionStage<Page> fetchPageFollowRedirect(WebURL curURL) {
        return fetchPage(curURL).thenCompose(page -> {
            if (isRedirect(page.getStatusCode())) {
                return fetchPage(WebURL.copyWithNewUrl(page.getRedirectedToUrl(), curURL));
            }
            return CompletableFuture.completedFuture(page);
        });
    }

    /**
     * Initializes the current instance of the crawler
     *
     * @param id              the id of this crawler instance
     * @param crawlController the controller that manages this crawling session
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public void init(int id, CrawlController crawlController) throws InstantiationException, IllegalAccessException {
        this.myId = id;
        this.pageFetcher = crawlController.getPageFetcher();
        this.robotstxtServer = crawlController.getRobotstxtServer();
        this.docServer = crawlController.getDocServer();
        this.frontier = crawlController.getFrontier();
        this.parser = crawlController.getParser();
        this.myController = crawlController;
        this.isWaitingForNewURLs = new AtomicBoolean(false);
    }

    /**
     * This function is called just before the termination of the current
     * crawler instance. It can be used for persisting in-memory data or other
     * finalization tasks.
     */
    public void onBeforeExit() {
        // Do nothing by default
        // Sub-classed can override this to add their custom functionality
    }

    /**
     * This function is called just before starting the crawl by this crawler
     * instance. It can be used for setting up the data structures or
     * initializations needed by this crawler instance.
     */
    public void onStart() {
        // Do nothing by default
        // Sub-classed can override this to add their custom functionality
    }

    // 기본 3시간 sleep
    public boolean shouldProcess(WebURL url) {
        return docServer.shouldProcess(url);
    }

    // 기본 1시간 sleep
    public boolean shouldSchedule(WebURL url) {
        return docServer.shouldSchedule(url);
    }

    /**
     * Classes that extends WebCrawler should overwrite this function to tell the
     * crawler whether the given url should be crawled or not. The following
     * default implementation indicates that all urls should be included in the crawl
     * except those with a nofollow flag.
     *
     * @param url           the url which we are interested to know whether it should be
     *                      included in the crawl or not.
     * @param referringPage The Page in which this url was found.
     * @return if the url should be included in the crawl it returns true,
     * otherwise false is returned.
     */
    public boolean shouldVisit(Page referringPage, WebURL url) {
        if (myController.getConfig().isRespectNoFollow()) {
            return !((referringPage != null && referringPage.getContentType() != null && referringPage.getContentType().contains("html") && ((HtmlParseData) referringPage.getParseData()).getMetaTagValue("robots").contains("nofollow")) || url.getAttribute("rel").contains("nofollow"));
        }

        return true;
    }

    /**
     * Classes that extends WebCrawler should overwrite this function to process
     * the content of the fetched and parsed page.
     *
     * @param page the page object that is just fetched and parsed.
     */
    public void visit(Page page) {
        // Do nothing by default
        // Sub-classed should override this to add their custom functionality
    }

    /**
     * This function is called once the header of a page is fetched. It can be
     * overridden by sub-classes to perform custom logic for different status
     * codes. For example, 404 pages can be logged, etc.
     *
     * @param webUrl            WebUrl containing the statusCode
     * @param statusCode        Html Status Code number
     * @param statusDescription Html Status COde description
     */
    protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
        // Do nothing by default
        // Sub-classed can override this to add their custom functionality
    }

    /**
     * This function is called before processing of the page's URL
     * It can be overridden by subclasses for tweaking of the url before processing it.
     * For example, http://abc.com/def?a=123 - http://abc.com/def
     *
     * @param curURL current URL which can be tweaked before processing
     * @return tweaked WebURL
     */
    protected WebURL handleUrlBeforeProcess(WebURL curURL) {
        return curURL;
    }

    /**
     * This function is called if the content of a url could not be fetched.
     *
     * @param page Partial page object
     */
    protected void onContentFetchError(Page page) {
        logger.warn("Can't fetch content of: {}", page.getWebURL().getURL());
        // Do nothing by default (except basic logging)
        // Sub-classed can override this to add their custom functionality
    }

    /**
     * This function is called if the content of a url is bigger than allowed size.
     *
     * @param urlStr - The URL which it's content is bigger than allowed size
     */
    protected void onPageBiggerThanMaxSize(String urlStr, long pageSize) {
        logger.warn("Skipping a URL: {} which was bigger ( {} ) than max allowed size", urlStr, pageSize);
    }

    /**
     * This function is called if there has been an error in parsing the content.
     *
     * @param webUrl URL which failed on parsing
     */
    protected void onParseError(WebURL webUrl) {
        logger.warn("Parsing error of: {}", webUrl.getURL());
        // Do nothing by default (Except logging)
        // Sub-classed can override this to add their custom functionality
    }

    /**
     * This function is called if the crawler encounters a page with a 3xx status code
     *
     * @param page Partial page object
     */
    protected void onRedirectedStatusCode(Page page) {
        //Subclasses can override this to add their custom functionality
    }

    /**
     * This function is called if the crawler encountered an unexpected http status code ( a
     * status code other than 3xx)
     *
     * @param url         URL in which an unexpected error was encountered while crawling
     * @param statusCode  Html StatusCode
     * @param contentType Type of Content
     * @param description Error Description
     */
    protected void onUnexpectedStatusCode(WebURL url, int statusCode, String contentType, String description) {
        logger.warn("Skipping URL: {}, StatusCode: {}, {}, {}", url, statusCode, contentType, description);
        // Do nothing by default (except basic logging)
        // Sub-classed can override this to add their custom functionality
    }

    /**
     * This function is called when a unhandled exception was encountered during fetching
     *
     * @param webUrl URL where a unhandled exception occured
     */
    protected void onUnhandledException(WebURL webUrl, Throwable e) {
        String urlStr = (webUrl == null ? "NULL" : webUrl.getURL());
        logger.warn("Unhandled exception while fetching {}: {}", urlStr, e.getMessage());
        logger.info("Stacktrace: ", e);
        // Do nothing by default (except basic logging)
        // Sub-classed can override this to add their custom functionality
    }

    protected void processDonePage200Sync(Page page, WebURL curURL) {
        if (shouldFollowLinksIn(page.getWebURL())) {
            ParseData parseData = page.getParseData();
            if (parseData == null)
                return;
            List<WebURL> toSchedule = new ArrayList<>();
            int maxCrawlDepth = myController.getConfig().getMaxDepthOfCrawling();
            for (WebURL webURL : parseData.getOutgoingUrls()) {
                webURL.setParentDocid(curURL.getDocid());
                webURL.setSessionId(curURL.getSessionId());
                webURL.setParentUrl(curURL.getURL());
                //                            int newdocid = docServer.getDocId(webURL.getURL());
                //                            if (newdocid > 0) {
                //                                // This is not the first time that this Url is seen.
                // So, we set the
                //                                // depth to a negative number.
                //                                webURL.setDepth((short) -1);
                //                                webURL.setDocid(newdocid);
                //                            } else {

                // TODO 이번 세션에서 본 url에 대해서 처리??? 어떻게 할지 고민하기
                webURL.setDocid(-1);
                webURL.setDepth((short) (curURL.getDepth() + 1));
                if ((maxCrawlDepth == -1) || (curURL.getDepth() < maxCrawlDepth)) {
                    if (shouldVisit(page, webURL)) {
                        if (robotstxtServer.allows(webURL)) {
                            if (shouldSchedule(webURL)) {
                                toSchedule.add(webURL);
                            }
                        } else {
                            logger.debug("Not visiting: {} as per the server's \"robots.txt\" " + "policy",
                                    webURL.getURL());
                        }
                    } else {
                        logger.debug("Not visiting: {} as per your \"shouldVisit\" policy", webURL.getURL());
                    }
                }
                //                            }
            }
            frontier.scheduleAll(toSchedule);
            toSchedule.stream().map(WebURL::getURL).forEach(docServer::setScheduled);
        } else {
            logger.debug("Not looking for links in page {}, " + "as per your \"shouldFollowLinksInPage\" " + "policy"
                    , page.getWebURL().getURL());
        }
    }

    protected void processDonePageRedirectSync(Page page, WebURL curURL) {
        if (myController.getConfig().isFollowRedirects()) {
            WebURL webURL = WebURL.copyWithNewUrl(page.getRedirectedToUrl(), curURL);
            if (shouldVisit(page, webURL)) {
                if (!shouldFollowLinksIn(webURL) || robotstxtServer.allows(webURL)) {
                    //                                    webURL.setDocid(docServer.getOrCreateDocID
                    // (movedToUrl));

                    if (shouldSchedule(webURL)) {
                        frontier.schedule(webURL);
                        docServer.setScheduled(webURL.getURL());
                    }
                } else {
                    logger.debug("Not visiting: {} as per the server's \"robots.txt\" policy", webURL.getURL());
                }
            } else {
                logger.debug("Not visiting: {} as per your \"shouldVisit\" policy", webURL.getURL());
            }
        }
    }

    protected CompletionStage<Page> processPage(WebURL curURL) {
        return fetchPage(curURL).thenApply((page) -> {
            if (page == null)
                return null;
            int statusCode = page.getStatusCode();
            if (statusCode < 200 || statusCode > 299) { // Not 2XX: 2XX status codes indicate success
                if (isRedirect(statusCode)) { // is 3xx  todo
                    processDonePageRedirectSync(page, curURL);
                }
            } else {
                processDonePage200Sync(page, curURL);
            }

            docServer.seen(page.getWebURL(), statusCode);
            return page;
        });
    }

    protected void processPage200Sync(Page page, WebURL curURL, PageFetchResult fetchResult) {

        if (!curURL.getURL().equals(fetchResult.getFetchedUrl())) {
            //                        if (docServer.isSeenBefore(fetchResult.getFetchedUrl())) {
            //                            logger.debug("Redirect page: {} has already been seen", curURL);
            //                            return;
            //                        }
            curURL.setURL(fetchResult.getFetchedUrl());
            if (!shouldVisit(page, curURL)) {
                logger.debug("Redirect page: {} has already been seen for a while", curURL);
                return;
            }
        }

        try {
            if (!page.applyPageFetchResult(fetchResult, myController.getConfig().getMaxDownloadSize())) {
                throw new ContentFetchException();
            }
        } catch (SocketTimeoutException e) {
            throw new ContentFetchException(e);
        }

        if (page.isTruncated()) {
            logger.warn("Warning: unknown page size exceeded max-download-size, truncated to: " + "({}), " + "at " +
                    "URL:" + " {}", myController.getConfig().getMaxDownloadSize(), curURL.getURL());
        }

        parser.parse(page, curURL.getURL());

        boolean noIndex =
                myController.getConfig().isRespectNoIndex() && page.getContentType() != null && page.getContentType().contains("html") && ((HtmlParseData) page.getParseData()).getMetaTagValue("robots").
                contains("noindex");

        if (!noIndex) {
            visit(page);
        }
    }

    protected void processPageErrorSync(WebURL curURL, PageFetchResult fetchResult) {

        String description = EnglishReasonPhraseCatalog.INSTANCE.getReason(fetchResult.getStatusCode(),
                Locale.ENGLISH); // Finds
        // the status reason for all known statuses
        String contentType = fetchResult.getEntity() == null ? "" : fetchResult.getEntity().getContentType() == null
                ? "" : fetchResult.getEntity().getContentType().getValue();
        onUnexpectedStatusCode(curURL, fetchResult.getStatusCode(), contentType, description);
    }

    protected void processPageRedirectSync(Page page, WebURL curURL, PageFetchResult fetchResult) {

        // follow https://issues.apache.org/jira/browse/HTTPCORE-389

        page.setRedirect(true);

        String movedToUrl = fetchResult.getMovedToUrl();
        if (movedToUrl == null) {
            logger.warn("Unexpected error, URL: {} is redirected to NOTHING", curURL);
            return;
        }
        page.setRedirectedToUrl(movedToUrl);
        onRedirectedStatusCode(page);
    }

    /**
     * Determine whether links found at the given URL should be added to the queue for crawling.
     * By default this method returns true always, but classes that extend WebCrawler can
     * override it in order to implement particular policies about which pages should be
     * mined for outgoing links and which should not.
     * <p>
     * If links from the URL are not being followed, then we are not operating as
     * a web crawler and need not check robots.txt before fetching the single URL.
     * (see definition at http://www.robotstxt.org/faq/what.html).  Thus URLs that
     * return false from this method will not be subject to robots.txt filtering.
     *
     * @param url the URL of the page under consideration
     * @return true if outgoing links from this page should be added to the queue.
     */
    protected boolean shouldFollowLinksIn(WebURL url) {
        return true;
    }

    private boolean isRedirect(int statusCode) {
        return (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY || statusCode == HttpStatus.SC_MULTIPLE_CHOICES || statusCode == HttpStatus.SC_SEE_OTHER || statusCode == HttpStatus.SC_TEMPORARY_REDIRECT || statusCode == 308);
    }
}
