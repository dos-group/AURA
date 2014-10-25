package de.tuberlin.aura.core.filesystem;


public class FileInputSplit implements InputSplit {

    private static final long serialVersionUID = 1L;

    private static final String[] EMPTY_ARR = new String[0];

    private int splitNumber;

    private String[] hostnames;

    private String file;

    private long start;

    private long length;

    // --------------------------------------------------------------------------------------------

    public FileInputSplit(int num, String file, long start, long length, String[] hosts) {

        this.splitNumber = num;

        this.hostnames = hosts == null ? EMPTY_ARR : hosts;

        this.file = file;

        this.start = start;

        this.length = length;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int getSplitNumber() {
        return this.splitNumber;
    }

    public String[] getHostnames() {
        return this.hostnames;
    }

    public String getPath() {
        return file;
    }

    public long getStart() {
        return start;
    }

    public long getLength() {
        return length;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return getSplitNumber() ^ (file == null ? 0 : file.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        else if (obj != null && obj instanceof FileInputSplit && super.equals(obj)) {
            FileInputSplit other = (FileInputSplit) obj;

            return this.start == other.start &&
                    this.length == other.length &&
                    (this.file == null ? other.file == null : (other.file != null && this.file.equals(other.file)));
        }
        else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "[" + getSplitNumber() + "] " + file + ":" + start + "+" + length;
    }
}