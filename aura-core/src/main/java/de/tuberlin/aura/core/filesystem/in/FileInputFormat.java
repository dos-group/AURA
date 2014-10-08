package de.tuberlin.aura.core.filesystem.in;


import java.io.IOException;
import java.util.*;

import de.tuberlin.aura.core.filesystem.FileInputSplit;
import de.tuberlin.aura.core.filesystem.LocatableInputSplitAssigner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class FileInputFormat<OT> implements InputFormat<OT, FileInputSplit> {

    // -------------------------------------- Constants -------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(FileInputFormat.class);

    private static final long serialVersionUID = 1L;

    /**
     * The fraction that the last split may be larger than the others.
     */
    private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

    /**
     * The splitLength is set to -1L for reading the whole split.
     */
    protected static final long READ_WHOLE_SPLIT_FLAG = -1L;


    // --------------------------------------------------------------------------------------------
    //  Variables for internal operation.
    // --------------------------------------------------------------------------------------------

    /**
     * The input stream reading from the input file.
     */
    protected transient FSDataInputStream stream;

    /**
     * The start of the split that this parallel instance must consume.
     */
    protected transient long splitStart;

    /**
     * The length of the split that this parallel instance must consume.
     */
    protected transient long splitLength;


    // --------------------------------------------------------------------------------------------
    //  The configuration parameters. Configured on the instance and serialized to be shipped.
    // --------------------------------------------------------------------------------------------

    /**
     * The path to the file that contains the input.
     */
    protected Path filePath;

    /**
     * The the minimal split size, set by the configure() method.
     */
    protected long minSplitSize = 0;

    /**
     * The desired number of splits, as set by the configure() method.
     */
    protected int numSplits = -1;

    /**
     * Some file input formats are not splittable on a block level (avro, deflate)
     * Therefore, the FileInputFormat can only read whole files.
     */
    protected boolean unsplittable = false;


    protected Configuration conf = new Configuration();

    // --------------------------------------------------------------------------------------------
    //  Constructors
    // --------------------------------------------------------------------------------------------

    public FileInputFormat() {}

    protected FileInputFormat(Path filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("The file path must not be null.");
        }
        this.filePath = filePath;
    }

    // --------------------------------------------------------------------------------------------
    //  Getters/setters for the configurable parameters
    // --------------------------------------------------------------------------------------------

    public Path getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("File path may not be null.");
        }

        // TODO The job-submission web interface passes empty args (and thus empty
        // paths) to compute the preview graph. The following is a workaround for
        // this situation and we should fix this.
        if (filePath.isEmpty()) {
            setFilePath(new Path(""));
            return;
        }

        setFilePath(new Path(filePath));
    }

    public void setFilePath(Path filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("File path may not be null.");
        }

        this.filePath = filePath;
    }

    public long getMinSplitSize() {
        return minSplitSize;
    }

    public void setMinSplitSize(long minSplitSize) {
        if (minSplitSize < 0) {
            throw new IllegalArgumentException("The minimum split size cannot be negative.");
        }

        this.minSplitSize = minSplitSize;
    }

    public int getNumSplits() {
        return numSplits;
    }

    public void setNumSplits(int numSplits) {
        if (numSplits < -1 || numSplits == 0) {
            throw new IllegalArgumentException("The desired number of splits must be positive or -1 (= don't care).");
        }

        this.numSplits = numSplits;
    }

    // --------------------------------------------------------------------------------------------
    // Getting information about the split that is currently open
    // --------------------------------------------------------------------------------------------

    /**
     * Gets the start of the current split.
     *
     * @return The start of the split.
     */
    public long getSplitStart() {
        return splitStart;
    }

    /**
     * Gets the length or remaining length of the current split.
     *
     * @return The length or remaining length of the current split.
     */
    public long getSplitLength() {
        return splitLength;
    }

    // --------------------------------------------------------------------------------------------
    //  Pre-flight: Configuration, Splits, Sampling
    // --------------------------------------------------------------------------------------------

    @Override
    public void configure(Configuration parameters) {
        conf = parameters;

        /*// get the file path
        String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
        if (filePath != null) {
            try {
                this.filePath = new Path(filePath);
            }
            catch (RuntimeException rex) {
                throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage());
            }
        }
        else if (this.filePath == null) {
            throw new IllegalArgumentException("File path was not specified in input format, or configuration.");
        }*/
    }


    @Override
    public LocatableInputSplitAssigner getInputSplitAssigner(FileInputSplit[] splits) {
        return new LocatableInputSplitAssigner(splits);
    }

    @Override
    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (minNumSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }

        // take the desired number of splits into account
        minNumSplits = Math.max(minNumSplits, this.numSplits);

        final Path path = this.filePath;
        final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

        // get all the files that are involved in the splits
        List<FileStatus> files = new ArrayList<FileStatus>();
        long totalLength = 0;

        final FileSystem fs = path.getFileSystem(conf);
        final FileStatus pathFile = fs.getFileStatus(path);

        if(!acceptFile(pathFile)) {
            throw new IOException("The given file does not pass the file-filter");
        }
        if (pathFile.isDir()) {
            // input is directory. list all contained files
            final FileStatus[] dir = fs.listStatus(path);
            for (int i = 0; i < dir.length; i++) {
                if (!dir[i].isDir() && acceptFile(dir[i])) {
                    files.add(dir[i]);
                    totalLength += dir[i].getLen();
                }
            }
        } else {

            files.add(pathFile);
            totalLength += pathFile.getLen();
        }
        // returns if unsplittable
        if(unsplittable) {
            int splitNum = 0;
            for (final FileStatus file : files) {
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
                Set<String> hosts = new HashSet<String>();
                for(BlockLocation block : blocks) {
                    hosts.addAll(Arrays.asList(block.getHosts()));
                }
                long len = file.getLen();

                FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath().toString(), 0, len,
                        hosts.toArray(new String[hosts.size()]));
                inputSplits.add(fis);
            }
            return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
        }


        final long maxSplitSize = (minNumSplits < 1) ? Long.MAX_VALUE : (totalLength / minNumSplits +
                (totalLength % minNumSplits == 0 ? 0 : 1));

        // now that we have the files, generate the splits
        int splitNum = 0;
        for (final FileStatus file : files) {

            final long len = file.getLen();
            final long blockSize = file.getBlockSize();

            final long minSplitSize;
            if (this.minSplitSize <= blockSize) {
                minSplitSize = this.minSplitSize;
            }
            else {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Minimal split size of " + this.minSplitSize + " is larger than the block size of " +
                            blockSize + ". Decreasing minimal split size to block size.");
                }
                minSplitSize = blockSize;
            }

            final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
            final long halfSplit = splitSize >>> 1;

            final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);

            if (len > 0) {

                // get the block locations and make sure they are in order with respect to their offset
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);

                Arrays.sort(blocks, new Comparator<BlockLocation>() {

                    @Override
                    public int compare(BlockLocation o1, BlockLocation o2) {
                        if (o1.getOffset() < o2.getOffset()) {
                            return -1;
                        } else {
                            if (o1.getOffset() < o2.getOffset()) {
                                return 1;
                            } else
                                return 0;
                        }
                    }
                });

                long bytesUnassigned = len;
                long position = 0;

                int blockIndex = 0;

                while (bytesUnassigned > maxBytesForLastSplit) {
                    // get the block containing the majority of the data
                    blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                    // create a new split
                    FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath().toString(), position, splitSize,
                            blocks[blockIndex].getHosts());
                    inputSplits.add(fis);

                    // adjust the positions
                    position += splitSize;
                    bytesUnassigned -= splitSize;
                }

                // assign the last split
                if (bytesUnassigned > 0) {
                    blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                    final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath().toString(), position,
                            bytesUnassigned, blocks[blockIndex].getHosts());
                    inputSplits.add(fis);
                }
            } else {
                // special case with a file of zero bytes size
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
                String[] hosts;
                if (blocks.length > 0) {
                    hosts = blocks[0].getHosts();
                } else {
                    hosts = new String[0];
                }
                final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath().toString(), 0, 0, hosts);
                inputSplits.add(fis);
            }
        }

        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
    }

    /**
     * A simple hook to filter files and directories from the input.
     * The method may be overridden. Hadoop's FileInputFormat has a similar mechanism and applies the
     * same filters by default.
     *
     * @param fileStatus
     * @return true, if the given file or directory is accepted
     */
    protected boolean acceptFile(FileStatus fileStatus) {
        final String name = fileStatus.getPath().getName();
        return !name.startsWith("_") && !name.startsWith(".");
    }

    /**
     * Retrieves the index of the <tt>BlockLocation</tt> that contains the part of the file described by the given
     * offset.
     *
     * @param blocks The different blocks of the file. Must be ordered by their offset.
     * @param offset The offset of the position in the file.
     * @param startIndex The earliest index to look at.
     * @return The index of the block containing the given position.
     */
    private int getBlockIndexForPosition(BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex) {
        // go over all indexes after the startIndex
        for (int i = startIndex; i < blocks.length; i++) {
            long blockStart = blocks[i].getOffset();
            long blockEnd = blockStart + blocks[i].getLength();

            if (offset >= blockStart && offset < blockEnd) {
                // got the block where the split starts
                // check if the next block contains more than this one does
                if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
                    return i + 1;
                } else {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("The given offset is not contained in the any block.");
    }

    // --------------------------------------------------------------------------------------------
    @Override
    public void open(FileInputSplit split) throws IOException {

        if (!(split instanceof FileInputSplit)) {
            throw new IllegalArgumentException("File Input Formats can only be used with FileInputSplits.");
        }

        final FileInputSplit fileSplit = (FileInputSplit) split;

        this.splitStart = fileSplit.getStart();
        this.splitLength = fileSplit.getLength();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Opening input split " + fileSplit.getPath() + " [" + this.splitStart + "," + this.splitLength + "]");
        }

        // open the split in an asynchronous thread
        final InputSplitOpenThread isot = new InputSplitOpenThread(conf, fileSplit, 20000);
        isot.start();

        try {
            this.stream = isot.waitForCompletion();
        } catch (Throwable t) {
            throw new IOException("Error opening the Input Split " + fileSplit.getPath() +
                    " [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
        }

        // get FSDataInputStream
        if (this.splitStart != 0) {
            this.stream.seek(this.splitStart);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.stream != null) {
            // close input stream
            this.stream.close();
            stream = null;
        }
    }

    public String toString() {
        return this.filePath == null ?
                "File Input (unknown file)" :
                "File Input (" + this.filePath.toString() + ')';
    }

    // ============================================================================================

    public static class InputSplitOpenThread extends Thread {

        private final FileInputSplit split;

        private volatile FSDataInputStream fdis;

        private volatile Throwable error;

        private volatile boolean aborted;

        private final Configuration conf;

        private final long timeout;

        public InputSplitOpenThread(Configuration conf, FileInputSplit split, long timeout) {
            super("Transient InputSplit Opener");
            setDaemon(true);

            this.conf = conf;

            this.split = split;

            this.timeout = timeout;
        }

        @Override
        public void run() {
            try {
                final FileSystem fs = FileSystem.get(conf);
                this.fdis = fs.open(new Path(this.split.getPath()));

                // check for canceling and close the stream in that case, because no one will obtain it
                if (this.aborted) {
                    final FSDataInputStream f = this.fdis;
                    this.fdis = null;
                    f.close();
                }
            }
            catch (Throwable t) {
                this.error = t;
            }
        }

        public FSDataInputStream waitForCompletion() throws Throwable {
            final long start = System.currentTimeMillis();
            long remaining = this.timeout;

            do {
                try {
                    // wait for the task completion
                    this.join(remaining);
                }
                catch (InterruptedException iex) {
                    // we were canceled, so abort the procedure
                    abortWait();
                    throw iex;
                }
            }
            while (this.error == null && this.fdis == null &&
                    (remaining = this.timeout + start - System.currentTimeMillis()) > 0);

            if (this.error != null) {
                throw this.error;
            }
            if (this.fdis != null) {
                return this.fdis;
            } else {
                // double-check that the stream has not been set by now. we don't know here whether
                // a) the opener thread recognized the canceling and closed the stream
                // b) the flag was set such that the stream did not see it and we have a valid stream
                // In any case, close the stream and throw an exception.
                abortWait();

                final boolean stillAlive = this.isAlive();
                final StringBuilder bld = new StringBuilder(256);
                for (StackTraceElement e : this.getStackTrace()) {
                    bld.append("\tat ").append(e.toString()).append('\n');
                }
                throw new IOException("Input opening request timed out. Opener was " + (stillAlive ? "" : "NOT ") +
                        " alive. Stack of split open thread:\n" + bld.toString());
            }
        }

        private final void abortWait() {
            this.aborted = true;
            final FSDataInputStream inStream = this.fdis;
            this.fdis = null;
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (Throwable t) {}
            }
        }
    }
}