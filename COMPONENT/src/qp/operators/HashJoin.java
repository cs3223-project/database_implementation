package qp.operators;

import qp.operators.Join;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

public class HashJoin extends Join {
    int batchsize; // Number of tuples per outbatch
    int leftIndex; // Indices of the join attribute in left table
    int rightIndex; // Indices of the join attribute in right table
    boolean isEndOfPartitionLeft; // Reach end of partition left
    boolean isEndOfPartitionRight; // Reach end of partition right
    boolean isEndOfBatchRight; // End of right batch
    int lcurs = 0; // Cursor for left buffer page
    int rcurs = 0; // Cursor for right buffer page
    int partitionIndex = 0; // Partition index
    int linkedListIndex = 0;
    Batch leftBatch;
    Batch rightBatch;
    Batch outBatch;
    Partition leftHasher;
    Partition rightHasher;

    static int filenum = 0;
    String rfname;

    List<String> leftPartitions; // File names of the left tables
    List<String> rightPartitions; // File names of the right tables
    ObjectInputStream leftInput;
    ObjectInputStream rightInput;
    String filePrefix = "temphashfile";

    Hashtable<Integer, LinkedList<Tuple>> innerHashTable; // Hashtable for left partition
    LinkedList<Tuple> linkedList;

    //define a put method for the inner hash table
    public LinkedList<Tuple> put(Hashtable<Integer, LinkedList<Tuple>> innerHashTable, Integer key, LinkedList<Tuple> list) {
        if (innerHashTable.containsKey(key)) {
            LinkedList<Tuple> v = innerHashTable.get(key);
            v.add(list.getFirst());
            return v;
        } else {
            innerHashTable.put(key, list);
            return null;
        }
    }

    public HashJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        isEndOfPartitionLeft = false;
        isEndOfPartitionRight = true;
        isEndOfBatchRight = true;
        partitionIndex = 0;
        rcurs = 0;

        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftIndex = left.getSchema().indexOf(leftattr);
            rightIndex = right.getSchema().indexOf(rightattr);
        }

        if(!left.open() || !right.open()){
            return false;
        }

        // Partition left table
        leftHasher = new Partition(left, numBuff - 1, filePrefix + "_left_");
        leftPartitions = leftHasher.partitionTable(leftIndex);
        // Partition right table
        rightHasher = new Partition(right, numBuff - 1, filePrefix + "_right_");
        rightPartitions = rightHasher.partitionTable(rightIndex);
        return true;
//        Batch rightpage;
//
//        if (!right.open()) {
//            return false;
//        } else {
//            /** If the right operator is not a base table then
//             ** Materialize the intermediate result from right
//             ** into a file
//             **/
//            filenum++;
//            rfname = "HJtemp-" + String.valueOf(filenum);
//            try {
//                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
//                while ((rightpage = right.next()) != null) {
//                    out.writeObject(rightpage);
//                }
//                out.close();
//            } catch (IOException io) {
//                System.out.println("HashJoin: Error writing to temporary file");
//                return false;
//            }
//            if (!right.close())
//                return false;
//        }
//        if (left.open())
//            return true;
//        else
//            return false;
    }

    public Batch next() {
        System.out.println("HashJoin:========================in next=======================");
        if(isEndOfPartitionLeft && isEndOfPartitionRight && partitionIndex == 0 && rcurs == 0) {
            return null;
        }

        outBatch = new Batch(batchsize);

        while (!outBatch.isFull()) {
            //last batch to return
            if (isEndOfPartitionLeft && isEndOfPartitionRight && partitionIndex == 0 && rcurs == 0) {
                if(!outBatch.isEmpty()) {
                    return outBatch;
                }else{
                    return null;
                }
            }

            //join each partition
            for (int parIndex = partitionIndex; parIndex < numBuff - 1; parIndex ++) {

                //read a new left partition and build hash table for it
                if (isEndOfPartitionRight){
                    try {
                        String fName = leftPartitions.get(parIndex);
                        leftInput = new ObjectInputStream(new FileInputStream(fName));
                        isEndOfPartitionLeft = false;
                    }catch (IOException io) {
                        System.err.println("HashJoin: read in left partition error");
                        System.exit(1);
                    }

                    //build hash table for left partition
                    innerHashTable = new Hashtable<>((numBuff - 2) * batchsize);
                    while (!isEndOfPartitionLeft){
                        try {
                            leftBatch = (Batch)leftInput.readObject();
                            int counter = 0;
                            while(counter < leftBatch.size()){
                                Tuple rec = leftBatch.get(counter);
                                Integer key = hashFunc(rec.dataAt(leftIndex));
                                LinkedList<Tuple> list = new LinkedList<Tuple>();
                                list.add(rec);
                                put(innerHashTable, key, list); //assume the entire partition can be held
                                counter++;
                            }
                        }catch (EOFException e) {
                            isEndOfPartitionLeft = true; ///whole left partition has been hashed
                            try {
                                leftInput.close();
                            } catch (Exception ee) {
                                System.err.println(ee.toString());
                                System.exit(1);
                            }
                        }catch (Exception other){
                            System.err.println(other.toString());
                            System.exit(1);
                        }
                    }

                    //read right partition
                    try{
                        rightInput = new ObjectInputStream(new FileInputStream(rightPartitions.get(parIndex)));
                        isEndOfPartitionRight = false;
                    }catch (Exception e){
                        System.err.println(e.toString());
                        System.exit(1);
                    }
                }

                while (!isEndOfPartitionRight) {
                    //read another page of right partition
                    if (isEndOfBatchRight) {
                        try{
                            rightBatch = (Batch)rightInput.readObject();
                            isEndOfBatchRight = false;
                        } catch (EOFException e){
                            isEndOfPartitionRight = true;
                            try{
                                rightInput.close();
                            }catch (Exception ee) {
                                System.err.println(ee.toString());
                                System.exit(1);
                            }
                        } catch (Exception e){
                            System.err.println(e.toString());
                            System.exit(1);
                        }
                    }

                    if (isEndOfPartitionRight) {
                        break;
                    }

                    //for each tuple in right partition, probe hash table of left partition
                    for (int j = rcurs ; j <  rightBatch.size(); j++ ) {
                        Tuple righttuple = rightBatch.get(j);
                        if (linkedListIndex == 0) {
                            Integer key = hashFunc(righttuple.dataAt(rightIndex));
                            if (innerHashTable.containsKey(key)) {
                                linkedList = innerHashTable.get(key);
                            } else {
                                continue;
                            }
                        }
                        // join
                        for(int m = linkedListIndex ; m < linkedList.size(); m++) {
                            Tuple lefttuple = linkedList.get(m);
                            if (lefttuple.checkJoin(righttuple, leftIndex, rightIndex)) {
                                Tuple outtuple = null;
                                outtuple = lefttuple.joinWith(righttuple);
                                outBatch.add(outtuple);

                                if (outBatch.isFull()) {
                                    if(m != linkedList.size() - 1){//not finish scanning the list
                                        linkedListIndex = m + 1;
                                        partitionIndex = parIndex;
                                        rcurs = j;
                                    }else if (j != rightBatch.size() - 1) {//finish scanning the list but not reach the end of right page
                                        linkedListIndex = 0;
                                        partitionIndex = parIndex;
                                        rcurs = j + 1;
                                    }else if (!isEndOfPartitionRight) {//finish scanning the list and reach the end of right page, but not reach the end of right partition
                                        linkedListIndex = 0;
                                        partitionIndex = parIndex;
                                        rcurs = 0;
                                        isEndOfBatchRight = true;
                                    }else if (parIndex != numBuff - 2) {//end of right partition, read next left partition
                                        linkedListIndex = 0;
                                        partitionIndex = parIndex + 1;
                                        rcurs = 0;
                                        isEndOfBatchRight = true;
                                    }else {//finish joining
                                        linkedListIndex = 0;
                                        partitionIndex = 0;
                                        rcurs = 0;
                                        isEndOfBatchRight = true;
                                    }
                                    return outBatch;
                                }
                            }
                        }
                        linkedListIndex = 0;
                    }
                    rcurs = 0;
                    isEndOfBatchRight = true;
                }
            }
            partitionIndex = 0;
        }
        return outBatch;
    }

    /* djb2
     * This algorithm was first reported by Dan Bernstein
     * many years ago in comp.lang.c
     */
    protected int hashFunc(Object o) {
        int hash = 5381;
        int len = String.valueOf(o).length();
        for(int i = 0; i < len; ++i){
            hash = 33* hash + String.valueOf(o).charAt(i);
        }
        return hash;
    }

    // Delete temp files
    public boolean close() {
        if (left.close() && right.close()) {
            for(int i = 0; i < numBuff -1; i++) {
                File fLeft= new File(filePrefix + "_left_" + i);
                File fRight = new File(filePrefix + "_right_" + i);
                fLeft.delete();
                fRight.delete();
            }
            return true;
        } else {
            return false;
        }
    }
}