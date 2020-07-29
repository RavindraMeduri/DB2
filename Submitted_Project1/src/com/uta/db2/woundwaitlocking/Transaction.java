package com.uta.db2.woundwaitlocking;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

public class Transaction {
    private String transactionId;
    private int timeStamp;
    private String transactionState;
    HashSet<String> items_held = new HashSet();
    Queue<Operation> waitingOperationsTrans = new LinkedList();

    
    public Transaction() {
    }

    public String getTransactionId() {
        return this.transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public int getTimeStamp() {
        return this.timeStamp;
    }

    public void setTimeStamp(int timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getTransactionState() {
        return this.transactionState;
    }

    public void setTransactionState(String transactionState) {
        this.transactionState = transactionState;
    }
}