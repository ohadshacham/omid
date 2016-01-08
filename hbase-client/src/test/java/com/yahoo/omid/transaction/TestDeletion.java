package com.yahoo.omid.transaction;

import org.apache.hadoop.hbase.client.*;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.testng.Assert.assertTrue;

public class TestDeletion extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestDeletion.class);

    byte[] famA = Bytes.toBytes(TEST_FAMILY);
    byte[] famB = Bytes.toBytes(TEST_FAMILY2);
    byte[] colA = Bytes.toBytes("testdataA");
    byte[] colB = Bytes.toBytes("testdataB");
    byte[] data1 = Bytes.toBytes("testWrite-1");
    byte[] data2 = Bytes.toBytes("testWrite-2");
    byte[] modrow = Bytes.toBytes("test-del" + 3);

    static class FamCol {
        final byte[] fam;
        final byte[] col;

        public FamCol(byte[] fam, byte[] col) {
            this.fam = fam;
            this.col = col;
        }

    }

    @Test
    public void runTestDeleteFamily() throws Exception {

        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;
        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famB, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.deleteFamily(famA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        AssertJUnit.assertEquals("ColA count should be equal to rowsWritten", rowsWritten, (int) count.get(famColA));
        AssertJUnit.assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA, famColB);
        AssertJUnit.assertEquals("ColA count should be equal to rowsWritten - 1", (rowsWritten - 1), (int) count.get(famColA));
        AssertJUnit.assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
    }

    @Test
    public void runTestDeleteColumn() throws Exception {

        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;

        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famA, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.deleteColumn(famA, colA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        AssertJUnit.assertEquals("ColA count should be equal to rowsWritten", rowsWritten, (int) count.get(famColA));
        AssertJUnit.assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA, famColB);
        AssertJUnit.assertEquals("ColA count should be equal to rowsWritten - 1", (rowsWritten - 1), (int) count.get(famColA));
        AssertJUnit.assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
    }

    /**
     * This test is very similar to #runTestDeleteColumn() but exercises Delete#deleteColumns()
     */
    @Test
    public void runTestDeleteColumns() throws Exception {

        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;

        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famA, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.deleteColumns(famA, colA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        AssertJUnit.assertEquals("ColA count should be equal to rowsWritten", rowsWritten, (int) count.get(famColA));
        AssertJUnit.assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA, famColB);

        AssertJUnit.assertEquals("ColA count should be equal to rowsWritten - 1", (rowsWritten - 1), (int) count.get(famColA));
        AssertJUnit.assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
    }

    @Test
    public void runTestDeleteRow() throws Exception {
        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;

        FamCol famColA = new FamCol(famA, colA);
        writeRows(tt, t1, rowsWritten, famColA);

        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        int rowsRead = countRows(rs);
        AssertJUnit.assertTrue("Expected " + rowsWritten + " rows but " + rowsRead + " found",
                rowsRead == rowsWritten);

        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        rowsRead = countRows(rs);
        AssertJUnit.assertTrue("Expected " + (rowsWritten - 1) + " rows but " + rowsRead + " found",
                rowsRead == (rowsWritten - 1));

    }

    @Test
    public void testDeletionOfNonExistingColumnFamilyDoesNotWriteToHBase() throws Exception {

        // --------------------------------------------------------------------
        // Setup initial environment for the test
        // --------------------------------------------------------------------
        TransactionManager tm = newTransactionManager();
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        Transaction tx1 = tm.begin();
        LOG.info("{} writing initial data created ", tx1);
        Put p = new Put(Bytes.toBytes("row1"));
        p.add(famA, colA, data1);
        txTable.put(tx1, p);
        tm.commit(tx1);

        // --------------------------------------------------------------------
        // Try to delete a non existing CF
        // --------------------------------------------------------------------
        Transaction deleteTx = tm.begin();
        LOG.info("{} trying to delete a non-existing family created ", deleteTx);
        Delete del = new Delete(Bytes.toBytes("row1"));
        del.deleteFamily(famB);
        // This delete should not put data on HBase
        txTable.delete(deleteTx, del);

        // --------------------------------------------------------------------
        // Check data has not been written to HBase
        // --------------------------------------------------------------------
        HTable table = new HTable(hbaseConf, TEST_TABLE);
        Get get = new Get(Bytes.toBytes("row1"));
        get.setTimeStamp(deleteTx.getTransactionId());
        Result result = table.get(get);
        assertTrue(result.isEmpty());

    }

    private int countRows(ResultScanner rs) throws IOException {
        int count;
        Result r = rs.next();
        count = 0;
        while (r != null) {
            count++;
            LOG.trace("row: " + Bytes.toString(r.getRow()) + " count: " + count);
            r = rs.next();
        }
        return count;
    }

    private void writeRows(TTable tt, Transaction t1, int rowcount, FamCol... famCols) throws IOException {
        for (int i = 0; i < rowcount; i++) {
            byte[] row = Bytes.toBytes("test-del" + i);

            Put p = new Put(row);
            for (FamCol col : famCols) {
                p.add(col.fam, col.col, data1);
            }
            tt.put(t1, p);
        }
    }

    private Map<FamCol, Integer> countColsInRows(ResultScanner rs, FamCol... famCols) throws IOException {
        Map<FamCol, Integer> colCount = new HashMap<FamCol, Integer>();
        Result r = rs.next();
        while (r != null) {
            for (FamCol col : famCols) {
                if (r.containsColumn(col.fam, col.col)) {
                    Integer c = colCount.get(col);

                    if (c == null) {
                        colCount.put(col, 1);
                    } else {
                        colCount.put(col, c + 1);
                    }
                }
            }
            r = rs.next();
        }
        return colCount;
    }

}