package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;
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
    private int pages = 0;

    private ObjectInputStream inputStreamIter;

    public OrderBy(Operator base, List<OrderType> orderTypes, int numBuffers) {
        super(OpType.ORDERBY);
        this.base = base;
        this.orderByTypeList = orderTypes;
        this.buffers = numBuffers;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public int getBuffers() {
        assert buffers != 0;
        return buffers;
    }

    public int getPages() {
        return pages;
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
            System.out.println("-------- OrderBy operator close --------");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return super.close();
    }

    public Object clone() {
        Operator clone = (Operator) base.clone();
        OrderBy cloneOB = new OrderBy(clone, orderByTypeList, buffers);
        cloneOB.setSchema((Schema) schema.clone());
        return cloneOB;
    }

    /**
     * The first phase of the external sort, we generate sorted runs by batches.
     */
    private void generateSortedRuns() {
        initTupleSize = 0;
        Batch current = base.next();

        if (current != null) {
            List<Batch> toRun = new ArrayList<>();
            int buffNo = 0;
            while (buffNo < buffers) {
                initTupleSize += current.size();
                toRun.add(current);
                current = base.next();
                if (current == null) {
                    break;
                }
                buffNo++;
            }

            // processing the batches to sorted runs
            List<Batch> sortedRunBatch = createSortedRun(toRun);
            File sortedRunFile = writeRunToFile(sortedRunBatch);
            sortedRuns.add(sortedRunFile);
        }

        pages = sortedRuns.size() * buffers;
    }

    /**
     * Create sorted runs from the batch and its tuples to be passed as a sorted run.
     * @param toRun batch to be run
     * @return the completed sorted run with the list of batches
     */
    private List<Batch> createSortedRun(List<Batch> toRun) {
        List<Tuple> tuples = new ArrayList<>(); // all tuples to be sorted
        List<Batch> runBatch = new ArrayList<>(); // create batch of main memory size

        for (int i = 0; i < toRun.size(); i++) {
            addTuplesFromBatch(tuples, toRun.get(i));
        }
        Collections.sort(tuples, tupleComparator);

        Batch batch = new Batch(batchRecordSize);
        for (Tuple t : tuples) {
            batch.add(t);
            if (batch.isFull()) {
                runBatch.add(batch);
                batch = new Batch(batchRecordSize);
            }
        }

        if (!batch.isFull()) {
            runBatch.add(batch);
        }

        return runBatch;
    }

    /**
     * Add Tuples in batch to the destination tuple list.
     * @param addToTuples Tuple list to be added to
     * @param batch to retrieve the tuples for adding
     */
    private void addTuplesFromBatch(List<Tuple> addToTuples, Batch batch) {
        int batchSize = batch.size();
        int i = 0;
        while (i < batchSize) {
            Tuple t = batch.get(i);
            addToTuples.add(t);
            i++;
        }
    }

    /**
     * Read Batch from ObjectInputStream.
     */
    private Batch readBatch(ObjectInputStream objInputStream) {
        try {
            return (Batch) objInputStream.readObject();
        } catch (EOFException e) {
            return null;
        } catch (ClassNotFoundException | IOException e) {
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

            int runNo = 0;
            while (runNo * availBuffers < numRuns) {
                int start = runNo * availBuffers;
                int end = Math.min((runNo + 1) * availBuffers, numRuns);
                List<File> runsToMerge = sortedRuns.subList(start, end);
                File merged = mergeSortedRuns(runsToMerge);
                newSortedRuns.add(merged);
                runNo++;
            }

            runNo++;
            refreshRuns(sortedRuns);
            sortedRuns = newSortedRuns;
        }
    }

    /**
     * Merge sorted runs into a longer sorted run.
     */
    private File mergeSortedRuns(List<File> runs) {
        int availBuffers = runs.size();
        List<Batch> inputBuffers = new ArrayList<>();
        List<ObjectInputStream> inputStreams = new ArrayList<>();

        if (runs.isEmpty()) {
            return null;
        }

        // open sorted runs files and add it to the input stream
        for (File f : runs) {
            try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
                inputStreams.add(ois);
            } catch (FileNotFoundException | IOException e) {
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

            int batchInBufferSize = inBuffers.size();
            int i = 0;

            while (i < batchInBufferSize) {
                Batch batch = inBuffers.get(i);
                int ptr = batchPointers[i];
                if (batchPointers[i] >= batch.size()) {
                    i++;
                    continue;
                }

                Tuple tuple = batch.get(ptr);
                if (smallest == null || tupleComparator.compare(tuple, smallest) < 0) {
                    smallest = tuple;
                    smallestIndex = i;
                }
                i++;
            }

            if (smallest == null) {
                break;
            }

            // updating smallest index and pointers
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

            if (!outBuffers.isEmpty() || outBuffers.isFull()) {
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
            String fileName = "tempSortedRun-" + runNo + "-" + fileNo;
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







