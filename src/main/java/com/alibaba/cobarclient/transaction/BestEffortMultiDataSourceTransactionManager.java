package com.alibaba.cobarclient.transaction;

import com.alibaba.cobarclient.Shard;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import java.util.Set;

public class BestEffortMultiDataSourceTransactionManager extends AbstractPlatformTransactionManager {

    private Set<Shard> shards;

    public BestEffortMultiDataSourceTransactionManager(Set<Shard> shards) {
        this.shards = shards;
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void doBegin(Object o, TransactionDefinition transactionDefinition) throws TransactionException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void doCommit(DefaultTransactionStatus defaultTransactionStatus) throws TransactionException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void doRollback(DefaultTransactionStatus defaultTransactionStatus) throws TransactionException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public Set<Shard> getShards() {
        return shards;
    }

    public void setShards(Set<Shard> shards) {
        this.shards = shards;
    }

}
