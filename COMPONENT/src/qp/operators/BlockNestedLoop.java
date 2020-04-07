package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BlockNestedLoop extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    int blockSize;
    int leftindex;                  // Indices of the join attributes in left table
    int rightindex;                 // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    String prefix = "BNJtemp-";     // The start name of file to be deleted
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

    public BlockNestedLoop(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = BufferManager.getBuffersPerJoin();
        blockSize = numBuff - 2;
    }

    //Setter and Getter
    public void setBlockSize(int size) {
        this.blockSize = size;
    }

    public int getBlockSize() {
        return blockSize;
    }

    /*
        for each block br in Br do
            for each block bs in Bs do
                for each tuple tr in Tr do
                    for each tuple ts in Ts do
                        compare(tr and ts) if they satisfied the condition add them in the result of the join
                    end
                end
            end
        end
    */
    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        //leftindex = new ArrayList<>();
        //rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex = left.getSchema().indexOf(leftattr);
            rightindex = right.getSchema().indexOf(rightattr);
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        //System.out.println("BlockNestedJoin:========================in next=======================");
        int i, j;
        if (eosl) {
            return null;
        }
        outbatch = new Batch(batchsize);
        Batch newLeft;

        while (!outbatch.isFull()) {
            if (lcurs == 0 && eosr == true) {
                /** new left block is to be fetched**/
                //leftbatch = (Batch) left.next();
                leftbatch = new ArrayList<>();
                /*if (leftbatch == null) {
                    eosl = true;
                    return outbatch;
                }*/
                for(i = 0; i < batchsize; i++){
                    newLeft = (Batch) left.next();
                    if(newLeft != null){
                        leftbatch.add(newLeft);
                    }
                }

                lTuplesInCurrBlk = new ArrayList<>();
                for(Batch page : leftbatch){
                    for(i = 0; i < page.size(); i++){
                        lTuplesInCurrBlk.add(page.get(i));
                    }
                }

                if(leftbatch.isEmpty() || leftbatch == null){
                    eosl = true;
                    return outbatch;
                }

                /** Whenever a new block came, we have to start the
                 ** scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }

            }
            while (eosr == false) {
                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }
                    for (i = lcurs; i < lTuplesInCurrBlk.size(); ++i) {
                        for (j = rcurs; j < rightbatch.size(); ++j) {
                            Tuple lefttuple = lTuplesInCurrBlk.get(i);
                            Tuple righttuple = rightbatch.get(j);
                            if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                Tuple outtuple = lefttuple.joinWith(righttuple);
                                outbatch.add(outtuple);
                                if (outbatch.isFull()) {
                                    if (i == lTuplesInCurrBlk.size() - 1 && j == rightbatch.size() - 1) {  //case 1
                                        lcurs = 0;
                                        rcurs = 0;
                                    } else if (i != lTuplesInCurrBlk.size() - 1 && j == rightbatch.size() - 1) {  //case 2
                                        lcurs = i + 1;
                                        rcurs = 0;
                                    } else if (i == lTuplesInCurrBlk.size() - 1 && j != rightbatch.size() - 1) {  //case 3
                                        lcurs = i;
                                        rcurs = j + 1;
                                    } else {
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }
                                    return outbatch;
                                }
                            }
                        }
                        rcurs = 0;
                    }
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }
}
