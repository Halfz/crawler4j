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

package edu.uci.ics.crawler4j.frontier;

import com.sleepycat.je.*;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @author Yasser Ganjisaffar
 */

public class DocServerImpl implements DocServer {
    private static final Logger logger = LoggerFactory.getLogger(DocServerImpl.class);
    private static final String DATABASE_NAME = "DocIDs";
    private final Database docIDsDB;
    private final Object mutex = new Object();

    private int lastDocID;

    public DocServerImpl(Environment env, CrawlConfig config) {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(config.isResumableCrawling());
        dbConfig.setDeferredWrite(!config.isResumableCrawling());
        lastDocID = 0;
        docIDsDB = env.openDatabase(null, DATABASE_NAME, dbConfig);
        if (config.isResumableCrawling()) {
            int docCount = getDocCount();
            if (docCount > 0) {
                logger.info("Loaded {} URLs that had been detected in previous crawl.", docCount);
                lastDocID = docCount;
            }
        }
    }

    /**
     * Returns the docid of an already seen url.
     *
     * @param url the URL for which the docid is returned.
     * @return the docid of the url if it is seen before. Otherwise -1 is returned.
     */
    @Override
    public int getDocId(String url) {
        synchronized (mutex) {
            OperationStatus result = null;
            DatabaseEntry value = new DatabaseEntry();
            try {
                DatabaseEntry key = new DatabaseEntry(url.getBytes());
                result = docIDsDB.get(null, key, value, null);

            } catch (Exception e) {
                logger.error("Exception thrown while getting DocID", e);
                return -1;
            }

            if ((result == OperationStatus.SUCCESS) && (value.getData().length > 0)) {
                return Util.byteArray2Int(value.getData());
            }

            return -1;
        }
    }

    @Override
    public long getLastScheduled(String url) {
        return getDocId(url) > 0 ? Long.MAX_VALUE : 0;
    }

    @Override
    public long getLastSeen(String url) {
        return getDocId(url) > 0 ? Long.MAX_VALUE : 0;
    }

    @Override
    public long now() {
        return (new Date()).getTime();
    }

    @Override
    public int getOrCreateDocID(String url) {
        synchronized (mutex) {
            try {
                // Make sure that we have not already assigned a docid for this URL
                int docID = getDocId(url);
                if (docID > 0) {
                    return docID;
                }

                ++lastDocID;
                docIDsDB.put(null, new DatabaseEntry(url.getBytes()), new DatabaseEntry(Util.int2ByteArray(lastDocID)));
                return lastDocID;
            } catch (Exception e) {
                logger.error("Exception thrown while getting new DocID", e);
                return -1;
            }
        }
    }

    @Override
    public void close() {
        try {
            docIDsDB.close();
        } catch (DatabaseException e) {
            logger.error("Exception thrown while closing DocServer", e);
        }
    }

    @Override
    public void seen(String url, int statusCode) {

    }

    @Override
    public void setScheduled(String url) {

    }

    private int getDocCount() {
        try {
            return (int) docIDsDB.count();
        } catch (DatabaseException e) {
            logger.error("Exception thrown while getting DOC Count", e);
            return -1;
        }
    }
}