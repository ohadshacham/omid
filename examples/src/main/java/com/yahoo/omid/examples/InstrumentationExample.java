/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.examples;

import com.yahoo.omid.metrics.CodahaleMetricsProvider;
import com.yahoo.omid.metrics.MetricsProvider;
import com.yahoo.omid.tools.hbase.HBaseLogin;
import com.yahoo.omid.transaction.HBaseTransactionManager;
import com.yahoo.omid.transaction.RollbackException;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionException;
import com.yahoo.omid.transaction.TransactionManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * ****************************************************************************************************************
 *
 *  Example code demonstrates client side instrumentation
 *
 * ****************************************************************************************************************
 *
 * Please @see{BasicExample} first
 */
public class InstrumentationExample {

    private static final Logger LOG = LoggerFactory.getLogger(InstrumentationExample.class);

    public static void main(String[] args) throws Exception {

        LOG.info("Parsing command line arguments");
        Configuration exampleConfig = Configuration.parse(args);


        String userTableName = exampleConfig.userTableName;
        byte[] family = Bytes.toBytes(exampleConfig.cfName);
        byte[] exampleRow1 = Bytes.toBytes("EXAMPLE_ROW1");
        byte[] exampleRow2 = Bytes.toBytes("EXAMPLE_ROW2");
        byte[] qualifier = Bytes.toBytes("MY_Q");
        byte[] dataValue1 = Bytes.toBytes("val1");
        byte[] dataValue2 = Bytes.toBytes("val2");

        //Logging in to Secure HBase if required
        HBaseLogin.loginIfNeeded(exampleConfig);
        org.apache.hadoop.conf.Configuration omidConfig = exampleConfig.toOmidConfig();

        LOG.info("Creating HBase Transaction Manager");
        List<String> metricsConfigs = Collections.singletonList("csv:./monitoring.csv:5:SECONDS");
        CodahaleMetricsProvider metricsProvider = CodahaleMetricsProvider.createCodahaleMetricsProvider(metricsConfigs);
        TransactionManager tm = HBaseTransactionManager.newBuilder()
                .withConfiguration(omidConfig)
                .withMetrics(metricsProvider)
                .build();

        LOG.info("Creating access to Transactional Table '{}'", userTableName);
        try (TTable tt = new TTable(omidConfig, userTableName)) {

            for (int i = 0; i < 1000; i++) {
                executeTransaction(userTableName, family, exampleRow1, exampleRow2, qualifier, dataValue1, dataValue2, tm, tt);
            }
        }

        tm.close();

    }

    private static void executeTransaction(String userTableName, byte[] family, byte[] exampleRow1, byte[] exampleRow2,
                                           byte[] qualifier, byte[] dataValue1, byte[] dataValue2,
                                           TransactionManager tm,
                                           TTable tt) throws TransactionException, IOException, RollbackException {
        Transaction tx = tm.begin();
        LOG.info("Transaction {} STARTED", tx);

        Put row1 = new Put(exampleRow1);
        row1.add(family, qualifier, dataValue1);
        tt.put(tx, row1);
        LOG.info("Transaction {} writing value in [TABLE:ROW/CF/Q] => {}:{}/{}/{} = {} ",
                 tx, userTableName, Bytes.toString(exampleRow1), Bytes.toString(family),
                 Bytes.toString(qualifier), Bytes.toString(dataValue1));

        Put row2 = new Put(exampleRow2);
        row2.add(family, qualifier, dataValue2);
        tt.put(tx, row2);
        LOG.info("Transaction {} writing value in [TABLE:ROW/CF/Q] => {}:{}/{}/{} = {} ",
                 tx, userTableName, Bytes.toString(exampleRow2), Bytes.toString(family),
                 Bytes.toString(qualifier), Bytes.toString(dataValue2));

        tm.commit(tx);
        LOG.info("Transaction {} COMMITTED", tx);
    }

}
