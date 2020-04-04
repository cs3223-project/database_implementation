package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Distinct extends Operator {

    Operator base;
    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    int blockSize;
    int count = 0;
    int leftindex;                  // Indices of the join attributes in left table
    int rightindex;                 // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    List<Batch> leftbatch;          // Buffer page for left input stream
    List<Tuple> lTuplesInCurrBlk;   //Tuples inside block
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    /*static int filenum = 0;         // To get unique filenum for this operation
    int blockSize;
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached*/

    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
    }

    //Setter and Getter
    public void setBlockSize(int size) {
        this.blockSize = size;
    }

    public int getBlockSize() {
        return blockSize;
    }



    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        return base.open();
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {

        Batch inbatch = base.next();
        count++;

        if (inbatch == null) {
            return null;
        }

        Batch outbatch = new Batch(batchsize);
        int curr = 0;
        while (curr < inbatch.size()) {
            outbatch.add(inbatch.get(curr));
            for (int j = 0; j < outbatch.size() - 1; j++) {
                Tuple outbatchTup = outbatch.get(j);
                Tuple inbatchTup = inbatch.get(curr);
                if (outbatchTup.data().equals(inbatchTup.data())) {
                    outbatch.remove(outbatch.size() - 1);
                    break;
                }
            }
            curr++;
        }

        base.open();

        Batch newInBatch = base.next();
        int newCount = 0;

        while (newInBatch != null) {
            if (newCount > count - 1) {
                for (int i = 0; i < outbatch.size(); i++) {
                    boolean hasDuplicate = false;
                    for (int j = 0; j < newInBatch.size(); j++) {
                        Tuple newOutBatchTup = outbatch.get(i);
                        Tuple newInBatchTup = newInBatch.get(j);
                        if (newOutBatchTup.data().equals(newInBatchTup.data())) {
                            outbatch.remove(outbatch.indexOf(newOutBatchTup));
                            System.out.println("removing duplicate");
                            hasDuplicate = true;
                            break;
                        }
                    }
                    if (hasDuplicate) {
                        i--;
                    }
                }
            }
            newInBatch = base.next();
            newCount++;
        }

        base.open();
        newCount = 0;
        while (newCount < count) {
            newInBatch = base.next();
            newCount++;
        }

        return outbatch;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
//        Scan newscan = new Scan(newbase, optype);
        Distinct newDist = new Distinct(newbase, optype);
        newDist.setSchema((Schema) schema.clone());
        return newDist;
    }

}
