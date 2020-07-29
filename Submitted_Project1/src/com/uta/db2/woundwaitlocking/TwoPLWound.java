package com.uta.db2.woundwaitlocking;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class TwoPLWound {
    static Integer timeStamp = 0;
    static HashMap<String, Transaction> transactionTableMap = new HashMap();
    static HashMap<String, Lock> lockTableMap = new HashMap();

    public TwoPLWound() {
    }

    // Performing the operations on transactions
    public static void parseInput(String line) {
        String inputOperation = line.substring(0, 1);
        String itemName;
        String transId;
        Transaction get_Transaction;
        String get_trans_State;
        String operation;
        Operation op;
        Lock lck;
        
        // Passing the input operation to switch case- b: begin, r: read, w: write, e: end/commit  
        switch(inputOperation.hashCode()) {
        case 98:
            if (inputOperation.equals("b")) {
                Transaction tr = new Transaction();
                tr.setTransactionId(line.substring(1, line.length() - 1));
                timeStamp = timeStamp + 1;
                tr.setTimeStamp(timeStamp);
                tr.setTransactionState("Active");
                transactionTableMap.put(tr.getTransactionId(), tr);
                System.out.println("Begin transaction T" + tr.getTransactionId() + " Timestamp - " + tr.getTimeStamp() + " State - " + tr.getTransactionState());
                return;
            }
            break;
        case 101:
            if (inputOperation.equals("e")) {
                itemName = line.substring(1, line.length() - 1);
                get_Transaction = (Transaction)transactionTableMap.get(itemName);
                get_trans_State = ((Transaction)transactionTableMap.get(itemName)).getTransactionState();
                op = new Operation("End", (String)null, itemName);
                if (get_trans_State.equals("Active")) {
                    get_Transaction.setTransactionState("Commit");
                    System.out.println("Transaction T" + itemName + " is committed and all the locks are released.");
                    HashSet<String> itemLocks = get_Transaction.items_held;
                    Iterator var17 = itemLocks.iterator();

                    while(var17.hasNext()) {
                        String item = (String)var17.next();
                        lck = (Lock)lockTableMap.get(item);
                        releaseLock(get_Transaction, lck);
                    }

                    return;
                } else {
                    if (get_trans_State.equals("Blocked")) {
                        System.out.println("Transaction T" + get_Transaction.getTransactionId() + " is already BLOCKED");
                        blocked(get_Transaction, op);
                    } else if (get_trans_State.equals("Abort")) {
                        System.out.println("Transaction T" + itemName + " is already aborted");
                        return;
                    }

                    return;
                }
            }
            break;
        case 114:
            if (inputOperation.equals("r")) {
                itemName = line.substring(line.indexOf(40) + 1, line.indexOf(41));
                transId = line.substring(1, line.indexOf(40));
                get_Transaction = (Transaction)transactionTableMap.get(transId);
                get_trans_State = ((Transaction)transactionTableMap.get(transId)).getTransactionState();
                operation = inputOperation.equals("r") ? "Read" : "Write";
                op = new Operation(operation, itemName, transId);
                if (get_trans_State.equals("Active")) {
                    if (lockTableMap.containsKey(itemName)) {
                        lck = (Lock)lockTableMap.get(itemName);
                        if (lck.getLockState() == null) {
                            lck.setLockState("Read");
                            lck.readLockTransId.add(transId);
                            get_Transaction.items_held.add(itemName);
                            System.out.println("Transaction T" + transId + " has been appended to readlock list for data item " + itemName + ". So, it has acquired 'Read Lock' on " + itemName);
                            return;
                        } else {
                            if (lck.getLockState().equals("Read") && operation.equals("Read")) {
                                readRead(itemName, get_Transaction, lck);
                            } else if (lck.getLockState().equals("Write") && operation.equals("Read")) {
                                writeRead(itemName, get_Transaction, lck, op);
                                return;
                            }

                            return;
                        }
                    } else {
                        lck = new Lock();
                        lck.setItemName(itemName);
                        lck.setLockState("Read");
                        lck.readLockTransId.add(transId);
                        System.out.println("Transaction State for transaction T" + transId + " is Active. So, Entry for DataItem " + itemName + " has been made in the lock table and transaction " + transId + " has acquired 'Read Lock' on it ");
                        get_Transaction.items_held.add(itemName);
                        lockTableMap.put(itemName, lck);
                        return;
                    }
                } else {
                    if (get_trans_State.equals("Blocked")) {
                        System.out.println("Transaction T" + get_Transaction.getTransactionId() + " is already BLOCKED");
                        if (!lockTableMap.containsKey(itemName)) {
                            lck = new Lock();
                            lck.setItemName(itemName);
                            lck.setLockState((String)null);
                            lck.setWriteLockTransId((String)null);
                            lockTableMap.put(itemName, lck);
                            System.out.println("Entry for DataItem " + itemName + " has been made in the lock table");
                        }

                        blocked(get_Transaction, op);
                    } else {
                        System.out.println("Transaction T" + transId + " is already aborted");
                    }

                    return;
                }
            }
            break;
        case 119:
            if (inputOperation.equals("w")) {
                itemName = line.substring(line.indexOf(40) + 1, line.indexOf(41));
                transId = line.substring(1, line.indexOf(40));
                get_Transaction = (Transaction)transactionTableMap.get(transId);
                get_trans_State = ((Transaction)transactionTableMap.get(transId)).getTransactionState();
                operation = inputOperation.equals("w") ? "Write" : "Read";
                op = new Operation(operation, itemName, transId);
                if (get_trans_State.equals("Active")) {
                    if (lockTableMap.containsKey(itemName)) {
                        lck = (Lock)lockTableMap.get(itemName);
                        if (lck.getLockState() == null) {
                            lck.setLockState("Write");
                            lck.setWriteLockTransId(transId);
                            get_Transaction.items_held.add(itemName);
                            System.out.println("Transaction T" + get_Transaction.getTransactionId() + " has been granted 'Write Lock' for data item " + itemName);
                            return;
                        } else {
                            if (lck.getLockState().equals("Read") && operation.equals("Write")) {
                                readWrite(itemName, get_Transaction, lck, op);
                            } else if (lck.getLockState().equals("Write") && operation.equals("Write")) {
                                writeWrite(itemName, get_Transaction, lck, op);
                                return;
                            }

                            return;
                        }
                    } else {
                        lck = new Lock();
                        lck.setItemName(itemName);
                        lck.setLockState("Write");
                        lck.setWriteLockTransId(transId);
                        System.out.println("Transaction State for transaction T" + transId + " is Active. So, Entry for DataItem " + itemName + " has been made in the lock table and transaction T" + transId + " has acquired 'Write Lock' on it ");
                        get_Transaction.items_held.add(itemName);
                        lockTableMap.put(itemName, lck);
                        return;
                    }
                } else {
                    if (get_trans_State.equals("Blocked")) {
                        System.out.println("Transaction T" + get_Transaction.getTransactionId() + " is already BLOCKED");
                        if (!lockTableMap.containsKey(itemName)) {
                            lck = new Lock();
                            lck.setItemName(itemName);
                            lck.setLockState((String)null);
                            lck.setWriteLockTransId((String)null);
                            lockTableMap.put(itemName, lck);
                            System.out.println("Entry for DataItem " + itemName + " has been made in the lock table");
                        }

                        blocked(get_Transaction, op);
                    } else if (get_trans_State.equals("Abort")) {
                        System.out.println("Transaction T" + transId + " is already aborted");
                        return;
                    }

                    return;
                }
            }
        }

        System.out.println("Provide a valid Input! End of the World!");
    }

    // Incoming operation is read and lock is already on read
    public static Lock readRead(String itemName, Transaction in_trans, Lock lock) {
        lock.readLockTransId.add(in_trans.getTransactionId());
        in_trans.items_held.add(itemName);
        System.out.println("Transaction T" + in_trans.getTransactionId() + " has been appended to readlock list for data item " + itemName + ". So, it has also acquired 'Read Lock' on " + itemName);
        return lock;
    }

    // Incoming operation is write and lock is already on read
    public static Lock readWrite(String itemName, Transaction in_trans, Lock lock, Operation op) {
        if (!lock.readLockTransId.isEmpty()) {
            String lock_transId;
            Transaction lock_trans;
            
            // read lock has only 1 operation
            if (lock.readLockTransId.size() == 1) {
                lock_transId = (String)lock.readLockTransId.peek();
                lock_trans = (Transaction)transactionTableMap.get(lock_transId);
                
                // compare transaction id's in both tables
                if (lock_transId.equals(in_trans.getTransactionId())) {
                    lock.setLockState("Write");
                    lock.setWriteLockTransId(in_trans.getTransactionId());
                    lock.readLockTransId.poll();
                    System.out.println("Lock upgraded to 'Write Lock' from 'Read Lock' for data item " + itemName + " on transaction id - " + in_trans.getTransactionId());
                } else {
                    System.out.println("Read Write conflict between T" + in_trans.getTransactionId() + "and T" + (String)lock.readLockTransId.peek());
                    wound_wait(itemName, in_trans, lock_trans, lock, op);
                }
            } else {
                System.out.println("Read Write conflict between T" + in_trans.getTransactionId() + " and multiple Transactions in readlock list");
                lock_transId = (String)lock.readLockTransId.peek();
                if (Integer.parseInt(in_trans.getTransactionId()) <= Integer.parseInt(lock_transId)) {
                    if (Integer.parseInt(in_trans.getTransactionId()) == Integer.parseInt(lock_transId)) {
                        lock.readLockTransId.poll();
                    }

                    System.out.println("Lock will be upgraded to 'Write Lock' from 'Read Lock' for data item " + itemName + " on transaction id - " + in_trans.getTransactionId());

                    while(!lock.readLockTransId.isEmpty()) {
                        lock_transId = (String)lock.readLockTransId.peek();
                        lock_trans = (Transaction)transactionTableMap.get(lock_transId);
                        wound_wait(itemName, in_trans, lock_trans, lock, op);
                    }
                } else {
                    lock_trans = (Transaction)transactionTableMap.get(lock_transId);
                    wound_wait(itemName, in_trans, lock_trans, lock, op);
                }
            }
        }

        return lock;
    }

    // Incoming operation is read and lock is on write
    public static Lock writeRead(String itemName, Transaction in_trans, Lock lock, Operation op) {
        if (!lock.getWriteLockTransId().equals(in_trans.getTransactionId())) {
            System.out.println("Write Read conflict between T" + in_trans.getTransactionId() + " and T" + lock.getWriteLockTransId());
            Transaction lock_trans = (Transaction)transactionTableMap.get(lock.getWriteLockTransId());
            wound_wait(itemName, in_trans, lock_trans, lock, op);
        }

        return lock;
    }

    // Incoming operation is write and lock is on write
    public static Lock writeWrite(String itemName, Transaction in_trans, Lock lock, Operation op) {
        if (!lock.getWriteLockTransId().equals(in_trans.getTransactionId())) {
            System.out.println("Write Write conflict between T" + in_trans.getTransactionId() + " and T" + lock.getWriteLockTransId());
            Transaction lock_trans = (Transaction)transactionTableMap.get(lock.getWriteLockTransId());
            wound_wait(itemName, in_trans, lock_trans, lock, op);
        }

        return lock;
    }

    // implementing wound wait method
    public static void wound_wait(String itemName, Transaction in_trans, Transaction lock_trans, Lock lock, Operation op) {
    	int tj = lock_trans.getTimeStamp();
        int ti = in_trans.getTimeStamp();
        
        // ti older than tj
        if (ti < tj) {
            System.out.println("T" + lock_trans.getTransactionId() + " 'ABORTED'");
            lock_trans.setTransactionState("Abort");
            aborted(lock_trans);
            if (op.operation.equals("Read")) {
                lock.setWriteLockTransId((String)null);
                lock.setLockState("Read");
                lock.readLockTransId.add(in_trans.getTransactionId());
                System.out.println("Transaction T" + in_trans.getTransactionId() + " has been appended to readlock list for data item " + itemName + ". So, it has acquired 'Read Lock' on " + itemName);
            } else {
                lock.readLockTransId.poll();
                lock.setLockState("Write");
                lock.setWriteLockTransId(in_trans.getTransactionId());
                System.out.println("Transaction T" + in_trans.getTransactionId() + " has been granted 'Write Lock' for data item " + itemName);
            }

            in_trans.items_held.add(itemName);
        } else {
            System.out.println("T" + ti + " 'BLOCKED'");
            in_trans.setTransactionState("Blocked");
            blocked(in_trans, op);
        }

    }

    // Keeping all the blocked transactions in waiting queue
    public static void blocked(Transaction in_trans, Operation op) {
        if (!op.operation.equals("End")) {
            Lock lock = (Lock)lockTableMap.get(op.dataItem);
            lock.waitingLockTrans.add(op);
            System.out.println("Transaction T" + in_trans.getTransactionId() + " is blocked and its op has been added to waiting list of lock on data item " + lock.getItemName() + ". T" + in_trans.getTransactionId() + " is waiting");
        } else {
            System.out.println("Transaction T" + in_trans.getTransactionId() + " is blocked and its \"End\" op has been added to its waiting list. T" + in_trans.getTransactionId() + " is waiting");
        }

        if (in_trans.waitingOperationsTrans.remove(op)) {
            System.out.println("========Op was already blocked requeue it to the end========");
        }

        in_trans.waitingOperationsTrans.add(op);
    }

    // Aborting all the transactions from tables 
    public static void aborted(Transaction trans) {
        HashSet<String> itemLocks = trans.items_held;
        Iterator var3 = itemLocks.iterator();

        while(var3.hasNext()) {
            String item = (String)var3.next();
            Lock lck = (Lock)lockTableMap.get(item);
            releaseLock(trans, lck);
        }

    }

    // Unlock all the items that has either been aborted/committed based on the Priority Queue, first waiting transaction gets released first
    public static void releaseLock(Transaction trans, Lock lock) {
        Transaction becameActive;
        if (lock.getLockState().equals("Read")) {
            if (lock.readLockTransId.size() == 1) {
                lock.readLockTransId.remove(trans.getTransactionId());
                lock.setLockState((String)null);
                becameActive = grantLock(lock);
                if (becameActive != null) {
                    startBlockedOps(becameActive);
                }
            } else {
                lock.readLockTransId.remove(trans.getTransactionId());
                becameActive = grantLock(lock);
                if (becameActive != null) {
                    startBlockedOps(becameActive);
                }
            }
        } else {
            lock.setWriteLockTransId((String)null);
            lock.setLockState((String)null);
            becameActive = grantLock(lock);
            if (becameActive != null) {
                startBlockedOps(becameActive);
            }
        }

    }

    // Granting locks to the item which are blocked 
    public static Transaction grantLock(Lock lock) {
        Transaction trans = null;

        while(!lock.waitingLockTrans.isEmpty()) {
            Operation op = (Operation)lock.waitingLockTrans.poll();
            trans = (Transaction)transactionTableMap.get(op.transId);
            String txnState = trans.getTransactionState();
            if (!txnState.equals("Abort") && !op.opCompleted) {
                trans.setTransactionState("Active");
                System.out.println("Transaction T" + trans.getTransactionId() + " has been UNBLOCKED now");
                if (lock.getLockState() == null) {
                    if (op.operation.equals("Read")) {
                        lock.setLockState("Read");
                        lock.readLockTransId.add(trans.getTransactionId());
                        System.out.println("Transaction T" + trans.getTransactionId() + " has been appended to readlock list for data item " + lock.getItemName() + ". So, it has acquired 'Read Lock' on " + lock.getItemName());
                        trans.items_held.add(lock.getItemName());
                        op.opCompleted = true;
                    } else {
                        lock.setLockState("Write");
                        lock.setWriteLockTransId(trans.getTransactionId());
                        System.out.println("Transaction T" + trans.getTransactionId() + " has been granted 'Write Lock' for data item " + lock.getItemName());
                        trans.items_held.add(lock.getItemName());
                        op.opCompleted = true;
                    }
                    break;
                }

                readWrite(op.dataItem, trans, lock, op);
                if (!trans.getTransactionState().equals("Blocked") || lock.waitingLockTrans.size() <= 1) {
                    if (trans.getTransactionState().equals("Blocked") && lock.waitingLockTrans.size() == 1) {
                        trans = null;
                    }
                    break;
                }
            }
        }

        return trans;
    }

    // all blocked transactions become active and then acquire read/write locks
    public static void startBlockedOps(Transaction trans) {
        label51:
        while(true) {
            if (trans.getTransactionState().equals("Active") && !trans.waitingOperationsTrans.isEmpty()) {
                Operation op = (Operation)trans.waitingOperationsTrans.poll();
                if (op.opCompleted) {
                    continue;
                }

                String itemName = op.dataItem;
                Lock lck;
                if (op.operation.equals("Read")) {
                    lck = (Lock)lockTableMap.get(itemName);
                    if (lck.getLockState() == null) {
                        lck.setLockState("Read");
                        lck.readLockTransId.add(trans.getTransactionId());
                        trans.items_held.add(itemName);
                        op.opCompleted = true;
                        System.out.println("Transaction T" + trans.getTransactionId() + " has been appended to readlock list for data item " + itemName + ". So, it has acquired 'Read Lock' on " + itemName);
                        continue;
                    }

                    if (lck.getLockState().equals("Read")) {
                        readRead(itemName, trans, lck);
                        op.opCompleted = true;
                        continue;
                    }

                    if (lck.getLockState().equals("Write")) {
                        writeRead(itemName, trans, lck, op);
                        op.opCompleted = true;
                    }
                    continue;
                }

                if (op.operation.equals("Write")) {
                    lck = (Lock)lockTableMap.get(itemName);
                    if (lck.getLockState() == null) {
                        lck.setLockState("Write");
                        lck.setWriteLockTransId(trans.getTransactionId());
                        trans.items_held.add(itemName);
                        op.opCompleted = true;
                        System.out.println("Transaction T" + trans.getTransactionId() + " has been granted 'Write Lock' for data item " + itemName);
                        continue;
                    }

                    if (lck.getLockState().equals("Read")) {
                        readWrite(itemName, trans, lck, op);
                        op.opCompleted = true;
                        continue;
                    }

                    if (lck.getLockState().equals("Write")) {
                        writeWrite(itemName, trans, lck, op);
                        op.opCompleted = true;
                    }
                    continue;
                }

                trans.setTransactionState("Commit");
                System.out.println("Transaction T" + trans.getTransactionId() + " is committed and all the locks are released.");
                HashSet<String> itemLocks = trans.items_held;
                Iterator var5 = itemLocks.iterator();

                while(true) {
                    if (!var5.hasNext()) {
                        continue label51;
                    }

                    String item = (String)var5.next();
                    lck = (Lock)lockTableMap.get(item);
                    releaseLock(trans, lck);
                }
            }

            return;
        }
    }

    // main method, change the file path and run the code
    public static void main(String[] args) throws IOException {
        String filePath = " ";
        String line = null;
        if (args.length > 0) {
            filePath = args[0];
        } else {
            filePath = "C:/Users/Ravindra-PC/Downloads/Database and Implementation Techniques/input";
        }

        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));

            while((line = br.readLine()) != null) {
                line = line.replace(" ", "");
                line = line.trim();
                System.out.println(line);
                parseInput(line);
            }
        } catch (FileNotFoundException var4) {
            var4.printStackTrace();
        }

    }
}