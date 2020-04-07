package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HashJoin extends Join {
    int batchSize;          // number of tuple in each outbatch

    int leftIndex;          // Index of the join att in left table
    int rightIndex;         // Index of the join att in right table

    String tempRFileName;  // File name where right table materialized
    static int filenum;     // Unique file number for the operation

    Batch outbatch;         // Buffer page for output

    int lcurs;              // Left table cur
    int rcurs;              // Right table cur
    int kcurs;              // Cursor for list containing search keys

    boolean eosl;           // Check if left table has been partitioned
    boolean eosr;           // Check if right table has been partitioned
    boolean checkKeySet;    // Check if keyset has been gotten during probing
    boolean checkHashJoin;  // Check if HashJoin is completed
    boolean build;          // Check if there is a need to partition again

    HashMap<Object, ArrayList<Tuple>> leftHashTable;
    HashMap<Object, ArrayList<Tuple>> rightHashTable;
    HashMap<Object, ArrayList<Tuple>> probingHashTable;

    Partition leftHasher;
    Partition rightHasher;

    List<Object> searchKeyList;

    public HashJoin(Join jn){
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open(){
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftIndex = left.getSchema().indexOf(leftattr);
            rightIndex = right.getSchema().indexOf(rightattr);
        }

        Batch rightPage;

        leftHashTable = new HashMap<>();
        rightHashTable = new HashMap<>();
        probingHashTable = new HashMap<>();

        /** Init curs input buffer **/
        lcurs = 0;
        rcurs = 0;
        kcurs = 0;

        eosl = false;
        eosr = false;
        checkKeySet = false;
        checkHashJoin = false;
        build = false;

        if(!right.open()){
            return false;
        } else {
            filenum++;
            tempRFileName = "tempHashJoin-" + filenum;
            try{
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempRFileName));
                while ((rightPage = right.next()) != null) {
                    out.writeObject(rightPage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("HashJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }

        leftHasher = new Partition(left, "_left_", leftHashTable);
        rightHasher = new Partition(right, tempRFileName, rightHashTable);

        if (left.open())
            return true;
        else
            return false;
    }

    public Batch next(){
        int left;
        int right;
        int key;

        if(checkHashJoin){
            close();
            return null;
        }

        outbatch = new Batch(batchSize);

        leftHashTable = leftHasher.partitionLeftTable(leftIndex);
        rightHashTable = rightHasher.partitionRightTable(rightIndex);

        if (!checkKeySet) {
            searchKeyList = new ArrayList<>();
            for(Object hashKey : leftHashTable.keySet()){
                searchKeyList.add(hashKey);
            }
            checkKeySet = true;
        }


        /**
         * Probing phase
         * Partition the left relation and build a hashmap using the final hashfunction
         * and probe the hashmap with the right relation
         */
        while (!outbatch.isFull() && kcurs < searchKeyList.size()) {
            while((key = kcurs) < searchKeyList.size()) {
                Object searchKey = searchKeyList.get(key++);
                if (rightHashTable.containsKey(searchKey)) {
                    ArrayList<Tuple> leftTupleList = leftHashTable.get(searchKey);
                    ArrayList<Tuple> rightTupleList = rightHashTable.get(searchKey);
                    /**
                     * build a hash table for the partition using final hash function
                     */
                    if (!build) {
                        //System.out.println("Building partition using searchKey: " + searchKey);
                        for (int i = 0; i < leftTupleList.size(); i++) {
                            Tuple leftTuple = leftTupleList.get(i);
                            int leftHash = SDBMHash(leftTuple.dataAt(leftIndex));
                            if (probingHashTable.containsKey(leftHash)) {
                                probingHashTable.get(leftHash).add(leftTuple);
                            } else {
                                ArrayList<Tuple> probeLeftList = new ArrayList<Tuple>();
                                probeLeftList.add(leftTuple);
                                probingHashTable.put(leftHash, probeLeftList);
                            }
                        }
                        build = true;
                        //System.out.println("build completed");
                    }

                    for (right = rcurs; right < rightTupleList.size(); right++) {
                        Tuple rightTuple = rightTupleList.get(right);
                        /**
                         * hash the right tuple with the final hash function and check for matches
                         * if there is matches, then all the tuples in the hashMap will be joined
                         * with the tuple tuple using the hashKey
                         */
                        int rightHash = SDBMHash(rightTuple.dataAt(rightIndex));
                        if (probingHashTable.containsKey(rightHash)) {
                            ArrayList<Tuple> probeLeftList = probingHashTable.get(rightHash);
                            for (left = lcurs; left < probeLeftList.size(); left++) {
                                Tuple leftTuple = probeLeftList.get(left);
                                Tuple outputTuple = leftTuple.joinWith(rightTuple);
                                //Debug.PPrint(outputTuple);
                                outbatch.add(outputTuple);

                                if (outbatch.isFull()) {
                                    //case 1 probeList and right partition exhausted,
                                    if (left == probeLeftList.size() - 1 && right == rightTupleList.size() - 1) {
                                        kcurs = key;
                                        lcurs = 0;
                                        rcurs = 0;
                                        build = false;
                                        probingHashTable.clear();
                                        //case 2 right partition is not probed completely;
                                    } else if (right != rightTupleList.size() - 1 && left == probeLeftList.size() - 1) {
                                        rcurs = right + 1;
                                        lcurs = 0;
                                        //other case where probeList is not probed completely against the right tuple
                                    } else {
                                        lcurs = left + 1;
                                    }
                                    return outbatch;
                                }
                            }
                        }
                        rcurs++;
                        lcurs = 0;
                    }
                }
                kcurs++;
                lcurs = 0;
                rcurs = 0;
                build = false;
                probingHashTable.clear();
            }
        }
        kcurs++;
        lcurs = 0;
        rcurs = 0;
        build = false;
        probingHashTable.clear();

        //if all searchKey is exhausted, the join is completed with no more tuples to compare
        if (kcurs >= searchKeyList.size()) {
            checkHashJoin = true;
        }
        return outbatch;
    }

    public int SDBMHash(Object o) {
        int hash = 0;
        String obj = String.valueOf(o);
        for(int i = 0; i < obj.length(); i++){
            hash = obj.charAt(i) + (hash << 6) + (hash << 16) - hash;
        }
        return hash;
    }

    public boolean close(){
        File f = new File(tempRFileName);
        f.delete();
        return true;
    }
}