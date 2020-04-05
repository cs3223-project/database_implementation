package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class Partition {
    int batchSize;
    int mod; // num of hash bucket
    boolean eos; // end of input
    Schema schema;
    String partitionFileName;
    Operator table;
    List<String> fileNames;

    public Partition(Operator table, int numBuff, String name){
        this.table = table;
        this.mod = numBuff;
        this.partitionFileName = name;
        this.schema = table.getSchema();
        fileNames = new ArrayList<>();
        eos = false;
    }

    public List<String> partitionTable(int attrIndex){
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;
        Batch tempBuffer;

        Batch[] parBuffer = new Batch[mod];
        for(int i = 0; i < parBuffer.length; i++){
            parBuffer[i] = new Batch(batchSize);
        }

        ObjectOutputStream[] outFiles = new ObjectOutputStream[mod];

        // 1 partition 1 output file
        for(int partitionIndex = 0; partitionIndex < parBuffer.length; partitionIndex++){
            try{
                fileNames.add(partitionIndex, (partitionFileName + partitionIndex));
                outFiles[partitionIndex] = new ObjectOutputStream(new FileOutputStream(fileNames.get(partitionIndex)));
            }catch (Exception e){
                System.err.println("HashJoin: Writing temporary files failed.");
                System.exit(1);
            }
        }

        // read a page of table and split into partitions
        while(!eos){
            tempBuffer = table.next();

            // put tuples into corresponding partition buffers
            Tuple record;
            int index;

            if(tempBuffer != null) {
                for (int i = 0; i < tempBuffer.size(); i++) {
                    record = tempBuffer.get(i);
                    index = Integer.valueOf((Integer) record.dataAt(attrIndex)) % mod;
                    parBuffer[index].add(record);

                    //if full, write out
                    if (parBuffer[index].isFull()) {
                        try {
                            outFiles[index].writeObject(parBuffer[index]);
                        } catch (Exception e) {
                            System.err.println("HashJoin: Writing to temporary file failed.");
                            System.exit(1);
                        }
                        parBuffer[index] = new Batch(batchSize);
                    }
                }
            }

            // end of table, output all
            if(tempBuffer == null){
                eos = true;
                // write out all non-empty buffers
                for(int i = 0; i < mod; i++){
                    try{
                        if(!parBuffer[i].isEmpty()){
                            outFiles[i].writeObject(parBuffer[i]);
                        }
                        outFiles[i].close();
                    }catch (Exception e){
                        System.err.print(e.toString());
                        System.exit(1);
                    }
                }
                return fileNames;
            }
        }
        return fileNames;
    }
}
