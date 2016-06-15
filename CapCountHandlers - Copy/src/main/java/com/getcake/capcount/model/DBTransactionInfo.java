package com.getcake.capcount.model;

import com.datastax.driver.core.BatchStatement;

public class DBTransactionInfo {
    public BatchStatement batchUpsertIncCountStmt, batchUpdateIncDateStmt, batchInsertIncReqStmt, batchDeleteDateStmt, batchDeleteCountStmt;

}
