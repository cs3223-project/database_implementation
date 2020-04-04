package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

/**
 * The OrderBy operator will order specified attributes and order type (ASC or DESC).
 * This is implemented using external sorting.
 */
public class OrderBy extends Operator {

    private Operator base;
    private List<OrderType> orderByTypeList;
    private int buffers;
    private int tupleByteSize;
    private int batchRecordSize; // per page
    private Comparator<Tuple> tupleComparator;

    private List<File> sortedRuns; // list of files containing sorted runs
    private int runNo; // keep track of the number of sorted runs produced
    private int initTupleSize;
    private int processedTuples;

    private ObjectInputStream inputStreamIter;

    public OrderBy(Operator base, List<OrderType> orderTypes, int buffers) {
        super(OpType.ORDERBY);
        this.base = base;
        this.orderByTypeList = orderTypes;
        this.buffers = buffers;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Opens the OrderBy operator and performs the necessary initialisation,
     * and the external sorting algorithm for the ordering by specified OrderTypes.
     * @return true if operator is successfully opened and executed.
     */
    public boolean open(){
        if (!base.open()) {
            return false;
        }

        System.out.println("-------- OrderBy Operator open --------");

        // Initialising OrderBy operator
        runNo = 0;
        sortedRuns = new ArrayList<>();
        tupleComparator = new OrderByComparator(base.getSchema(), orderByTypeList);
        tupleByteSize = base.schema.getTupleSize();
        batchRecordSize = Batch.getPageSize() / tupleByteSize;

        // generate sorted runs using external sorting
        generateSortedRuns();

        // merge sorted runs
        performMerge();

        return true;
    }

    /**
     * Read and return next batch of tuples from the inputStreamIter.
     */
    public Batch next() {
        try {
            if (inputStreamIter == null) {
                File batchFile = sortedRuns.get(0);
                inputStreamIter = new ObjectInputStream(new FileInputStream(batchFile));
            }
            return readBatch(inputStreamIter);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Close operator: refresh sorted runs, close stream.
     */
    public boolean close() {
        refreshRuns(sortedRuns);
        try {
            inputStreamIter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return super.close();
    }

    /**
     * The first phase of the external sort, we generate sorted runs by batches.
     */
    private void generateSortedRuns() {
        initTupleSize = 0;
        Batch current = base.next();

        while (current != null) {
            List<Batch> toRun = new ArrayList<>();
            for (int i = 0; i < buffers; i++) {
                initTupleSize += current.size();
                toRun.add(current);
                current = base.next();
            }
            // processing the batches to sorted runs
            List<Batch> sortedRunBatch = createSortedRun(toRun);
            File sortedRunFile = writeRunToFile(sortedRunBatch);
            sortedRuns.add(sortedRunFile);
        }

    }

    /**
     * Create sorted runs from the batch and its tuples to be passed as a sorted run.
     * @param toRun batch to be run
     * @return the completed sorted run with the list of batches
     */
    private List<Batch> createSortedRun(List<Batch> toRun) {
        List<Tuple> tuples = new ArrayList<>();
        for (Batch b : toRun) {
            addTuplesFromBatch(tuples, b);
        }
        Collections.sort(tuples, tupleComparator);

        // create batch of main memory size
        List<Batch> runBatch = new ArrayList<>();
        Batch current = new Batch(batchRecordSize);
        for (Tuple t : tuples) {
            current.add(t);
            if (current.isFull()) {
                runBatch.add(current);
                current = new Batch(batchRecordSize);
            }
        }

        if (!current.isFull()) {
            runBatch.add(current);
        }

        return runBatch;
    }

    /**
     * Add Tuples in batch to the destination tuple list.
     * @param addToTuples Tuple list to be added to
     * @param batch to retrieve the tuples for adding
     */
    private void addTuplesFromBatch(List<Tuple> addToTuples, Batch batch) {
        for (int i = 0; i < batch.size(); i++) {
            Tuple t = batch.get(i);
            addToTuples.add(t);
        }
    }



    /**
     * Read Batch from ObjectInputStream.
     */
    private Batch readBatch(ObjectInputStream objInputStream) {
        try {
            Batch batch = (Batch) objInputStream.readObject();
            return batch;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Clear all files in sortedRuns.
     */
    private void refreshRuns(List<File> toBeCleared) {
        for (File f : toBeCleared) {
            f.delete();
        }
    }

    /**
     * The second phase merges sorted runs into a growing bigger file,
     * until all sorted runs are merged into an output file.
     */
    private void performMerge() {
        int availBuffers = buffers - 1;
        while (sortedRuns.size() > 1) {
            processedTuples = 0;
            int numRuns = sortedRuns.size();
            List<File> newSortedRuns = new ArrayList<>();

            for (int r = 0; r * availBuffers < numRuns; r++) {
                int start = r * availBuffers;
                int end = (r + 1) * availBuffers;
                end = Math.min(end, numRuns);

                List<File> runsToMerge = sortedRuns.subList(start, end);
                File merged = mergeSortedRuns(runsToMerge);
                newSortedRuns.add(merged);
            }

            runNo++;
            assert initTupleSize == processedTuples;
            refreshRuns(sortedRuns);
            sortedRuns = newSortedRuns;
        }
    }

    /**
     * Merge sorted runs into a longer sorted run.
     */
    private File mergeSortedRuns(List<File> runs) {
        if (runs.isEmpty()) {
            return null;
        }

        int availBuffers = runs.size();
        List<Batch> inputBuffers = new ArrayList<>();
        List<ObjectInputStream> inputStreams = new ArrayList<>();

        // open sorted runs files and add it to the input stream
        for (File f : runs) {
            try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
                inputStreams.add(ois);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // read batches from the input stream into the buffer
        for (ObjectInputStream s : inputStreams) {
            Batch b = readBatch(s);
            inputBuffers.add(b);
        }

        File mergedFile = sortRuns(inputBuffers, inputStreams, availBuffers);
        return mergedFile;

    }

    /**
     * Sort tuples in runs according to the tupleComparator.
     */
    private File sortRuns(List<Batch> inBuffers, List<ObjectInputStream> inStreams, int buffers) {
        Batch outBuffers = new Batch(batchRecordSize);
        File mergedFile = null;
        int[] batchPointers = new int[buffers];

        while (true) {
            Tuple smallest = null;
            int smallestIndex = 0;
            for (int i = 0; i < inBuffers.size(); i++) {
                Batch batch = inBuffers.get(i);
                int ptr = batchPointers[i];
                if (batchPointers[i] >= batch.size()) {
                    continue;
                }

                Tuple tuple = batch.get(ptr);
                boolean isTupleSmallest = tupleComparator.compare(tuple, smallest) < 0;
                if (smallest == null || isTupleSmallest) {
                    smallest = tuple;
                    smallestIndex = i;
                }
            }

            if (smallest == null) {
                break;
            }

            batchPointers[smallestIndex] += 1;
            if (batchPointers[smallestIndex] == inBuffers.get(smallestIndex).capacity()) {
                ObjectInputStream ois = inStreams.get(smallestIndex);
                Batch batch = readBatch(ois);
                if (batch != null) {
                    inBuffers.set(smallestIndex, batch);
                    batchPointers[smallestIndex] = 0;
                }
            }

            outBuffers.add(smallest);
            processedTuples++;

            if (outBuffers.isFull()) {
                if (mergedFile == null) {
                    mergedFile = writeRunToFile(Arrays.asList(outBuffers));
                } else {
                    appendRunToFile(outBuffers, mergedFile);
                }
            }

            if (!outBuffers.isEmpty()) {
                if (mergedFile == null) {
                    mergedFile = writeRunToFile(Arrays.asList(outBuffers));
                } else {
                    appendRunToFile(outBuffers, mergedFile);
                }
            }
        }
        return mergedFile;
    }

    /**
     * Writes the batches of sorted run to a temporary file.
     */
    private File writeRunToFile(List<Batch> sortedRun) {
        runNo++;
        int fileNo = 0;
        File sortedRunFile = null;

        try {
            fileNo++;
            //int numTuples = 0;
            String fileName = "SortedRun-" + runNo + "-" + fileNo;
            sortedRunFile = new File(fileName);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(sortedRunFile));

            for (Batch b : sortedRun) {
                out.writeObject(b);
                //numTuples += b.size();
            }
            fileNo++;
            out.close();
            return sortedRunFile;
        } catch (FileNotFoundException e) {
            System.err.println("OrderBy: File not found: " + sortedRunFile.getName());
        } catch (IOException e) {
            System.err.println("OrderBy: IO Error in writing sorted run to file.");
        }
        return sortedRunFile;
    }

    /**
     * Adds sorted run batch to a combined file, as part of the merging process.
     */
    private void appendRunToFile(Batch runBatch, File destFile) {
        try {
            ObjectOutputStream out = new AppendObjectOutputStream(new FileOutputStream(destFile, true));
            out.writeObject(runBatch);
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Altered ObjectOutputStream methods to allow appending streams to join files.
     */
    class AppendObjectOutputStream extends ObjectOutputStream {
        AppendObjectOutputStream(OutputStream outStream) throws IOException {
            super(outStream);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            reset();
        }
    }
}







