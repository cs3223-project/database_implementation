package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Partition {
    int batchSize;
    boolean eos;                                    // End of input stream
    Schema schema;
    Operator table;
    String currTable;                               // Name of current working table
    HashMap<Object, ArrayList<Tuple>> partition;    // HashMap partition to be returned
    ObjectInputStream in;                           // Read temp file for right partition

    public Partition(Operator table, String name, HashMap<Object, ArrayList<Tuple>> hashTable) {
        this.table = table;
        this.schema = table.getSchema();
        this.currTable = name;
        partition = hashTable;
        eos = false;
    }

    public HashMap<Object, ArrayList<Tuple>> partitionLeftTable(int attrIndex) {
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;
        Batch tempBuffer;

        // read a page of table and split into partitions
        while (!eos) {
            tempBuffer = table.next();

            // put tuples into corresponding partition buffers
            Tuple record;

            //System.out.println("Start partition " + currTable);
            while (tempBuffer != null) {
                for (int i = 0; i < tempBuffer.size(); i++) {
                    record = tempBuffer.get(i);
                    int hashKey = hashFunc(record.dataAt(attrIndex));
                    if(partition.containsKey(hashKey)){
                        partition.get(hashKey).add(record);
                    } else {
                        ArrayList<Tuple> tupleList = new ArrayList<>();
                        tupleList.add(record);
                        partition.put(hashKey, tupleList);
                    }
                }
                tempBuffer = table.next();
            }
            //System.out.println("Finished Patitioning " + currTable);
            eos = true;
        }
        return partition;
    }

    public HashMap<Object, ArrayList<Tuple>> partitionRightTable(int attrIndex) {
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;
        Batch tempBuffer;
        Tuple record;

        // read a page of table and split into partitions
        while (!eos) {
            try {
                in = new ObjectInputStream(new FileInputStream(currTable));
                //System.out.println("Start partitioning right table");
            } catch (Exception io){
                System.err.println("HashJoin: error reading file");
                System.exit(1);
            }
            try{
                tempBuffer = (Batch) in.readObject();
                while (tempBuffer != null) {
                    for (int i = 0; i < tempBuffer.size(); i++) {
                        record = tempBuffer.get(i);
                        int hashKey = hashFunc(record.dataAt(attrIndex));
                        if(partition.containsKey(hashKey)){
                            partition.get(hashKey).add(record);
                        } else {
                            ArrayList<Tuple> tupleList = new ArrayList<>();
                            tupleList.add(record);
                            partition.put(hashKey, tupleList);
                        }
                    }
                    tempBuffer = (Batch) in.readObject();
                }
            }catch (EOFException e) {
                try {
                    in.close();
                } catch (IOException io) {
                    System.out.println("HashJoin:Error in temporary file reading");
                }
                eos = true;
            } catch (ClassNotFoundException c) {
                System.out.println("HashJoin:Some error in deserialization ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("HashJoin:temporary file reading error");
                System.exit(1);
            }
            eos = true;
            //System.out.println("Right Table is partitioned " + currTable);
        }
        return partition;
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
}
