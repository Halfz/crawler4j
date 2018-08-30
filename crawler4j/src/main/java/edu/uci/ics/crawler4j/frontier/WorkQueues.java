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

import com.google.common.primitives.Bytes;
import com.sleepycat.je.*;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.Util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Yasser Ganjisaffar
 */
public class WorkQueues {
    protected final Object mutex = new Object();
    private final Database urlsDB;
    private final Environment env;
    private final boolean resumable;
    private final WebURLTupleBinding webURLBinding;

    public WorkQueues(Environment env, String dbName, boolean resumable) {
        this.env = env;
        this.resumable = resumable;
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(resumable);
        dbConfig.setDeferredWrite(!resumable);
        urlsDB = env.openDatabase(null, dbName, dbConfig);
        webURLBinding = new WebURLTupleBinding();
    }

    public long getLength() {
        return urlsDB.count();
    }

    public static String generateMd5(String in) {
        if (in == null) {
            return null;
        } else {
            try {
                MessageDigest digest = MessageDigest.getInstance("MD5");
                digest.reset();
                digest.update(in.getBytes());
                byte[] a = digest.digest();
                int len = a.length;
                StringBuilder sb = new StringBuilder(len << 1);
                int var6 = a.length;

                for (int var7 = 0; var7 < var6; ++var7) {
                    byte anA = a[var7];
                    sb.append(Character.forDigit((anA & 240) >> 4, 16));
                    sb.append(Character.forDigit(anA & 15, 16));
                }

                return sb.toString();
            } catch (NoSuchAlgorithmException var9) {
                throw new RuntimeException(var9);
            }
        }
    }

    public void close() {
        urlsDB.close();
    }

    public void delete(int count) {
        synchronized (mutex) {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            Transaction txn = beginTransaction();
            try (Cursor cursor = openCursor(txn)) {
                OperationStatus result = cursor.getFirst(key, value, null);
                int matches = 0;
                while ((matches < count) && (result == OperationStatus.SUCCESS)) {
                    cursor.delete();
                    matches++;
                    result = cursor.getNext(key, value, null);
                }
            }
            commit(txn);
        }
    }

    public List<WebURL> get(int max) {
        synchronized (mutex) {
            List<WebURL> results = new ArrayList<>(max);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            Transaction txn = beginTransaction();
            try (Cursor cursor = openCursor(txn)) {
                OperationStatus result = cursor.getFirst(key, value, null);
                int matches = 0;
                while ((matches < max) && (result == OperationStatus.SUCCESS)) {
                    if (value.getData().length > 0) {
                        results.add(webURLBinding.entryToObject(value));
                        matches++;
                    }
                    result = cursor.getNext(key, value, null);
                }
            }
            commit(txn);
            return results;
        }
    }

    public void put(WebURL url) {
        DatabaseEntry value = new DatabaseEntry();
        webURLBinding.objectToEntry(url, value);
        Transaction txn = beginTransaction();
        urlsDB.put(txn, getDatabaseEntryKey(url), value);
        commit(txn);
    }

    protected static void commit(Transaction tnx) {
        if (tnx != null) {
            tnx.commit();
        }
    }

    /*
     * The key that is used for storing URLs determines the order
     * they are crawled. Lower key values results in earlier crawling.
     * Here our keys are 6 bytes. The first byte comes from the URL priority.
     * The second byte comes from depth of crawl at which this URL is first found.
     * The rest of the 4 bytes come from the docid of the URL. As a result,
     * URLs with lower priority numbers will be crawled earlier. If priority
     * numbers are the same, those found at lower depths will be crawled earlier.
     * If depth is also equal, those found earlier (therefore, smaller docid) will
     * be crawled earlier.
     */
    protected static DatabaseEntry getDatabaseEntryKey(WebURL url) {
        byte[] md5Bytes = generateMd5(url.getURL()).getBytes();

        byte[] keyData = new byte[2];
        keyData[0] = url.getPriority();
        keyData[1] = ((url.getDepth() > Byte.MAX_VALUE) ? Byte.MAX_VALUE : (byte) url.getDepth());
//        Util.putIntInByteArray(url.getDocid(), keyData, 2);
//
//
//        byte[] b = new byte[4];
//        for (int i = 0; i < 4; i++) {
//            int offset = (3 - i) * 8;
//            b[i] = (byte) ((value >>> offset) & 0xFF);
//        }


        return new DatabaseEntry(Bytes.concat(keyData, md5Bytes));
    }

    protected Transaction beginTransaction() {
        return resumable ? env.beginTransaction(null, null) : null;
    }

    protected Cursor openCursor(Transaction txn) {
        return urlsDB.openCursor(txn, null);
    }
}