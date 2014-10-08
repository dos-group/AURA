package de.tuberlin.aura.core.filesystem;

import de.tuberlin.aura.core.topology.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public final class LocatableInputSplitAssigner implements InputSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(LocatableInputSplitAssigner.class);

    private final Set<FileInputSplit> unassigned = new HashSet<>();

    private final ConcurrentHashMap<String, List<FileInputSplit>> localPerHost = new ConcurrentHashMap<>();

    private int localAssignments;		// lock protected by the unassigned set lock

    private int remoteAssignments;		// lock protected by the unassigned set lock

    // --------------------------------------------------------------------------------------------

    public LocatableInputSplitAssigner(Collection<FileInputSplit> splits) {
        this.unassigned.addAll(splits);
    }

    public LocatableInputSplitAssigner(FileInputSplit[] splits) {
        Collections.addAll(this.unassigned, splits);
    }

    // --------------------------------------------------------------------------------------------

    public InputSplit getNextInputSplit(String host) {
        // for a null host, we return an arbitrary split
        if (host == null) {

            synchronized (this.unassigned) {
                Iterator<FileInputSplit> iter = this.unassigned.iterator();
                if (iter.hasNext()) {
                    FileInputSplit next = iter.next();
                    iter.remove();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Assigning arbitrary split to null host.");
                    }

                    remoteAssignments++;
                    return next;
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No more input splits remaining.");
                    }
                    return null;
                }
            }
        }

        host = host.toLowerCase(Locale.US);

        // for any non-null host, we take the list of non-null splits
        List<FileInputSplit> localSplits = this.localPerHost.get(host);

        // if we have no list for this host yet, create one
        if (localSplits == null) {
            localSplits = new ArrayList<FileInputSplit>(16);

            // lock the list, to be sure that others have to wait for that host's local list
            synchronized (localSplits) {
                List<FileInputSplit> prior = this.localPerHost.putIfAbsent(host, localSplits);

                // if someone else beat us in the case to create this list, then we do not populate this one, but
                // simply work with that other list
                if (prior == null) {
                    // we are the first, we populate

                    // first, copy the remaining splits to release the lock on the set early
                    // because that is shared among threads
                    FileInputSplit[] remaining;
                    synchronized (this.unassigned) {
                        remaining = (FileInputSplit[]) this.unassigned.toArray(new FileInputSplit[this.unassigned.size()]);
                    }

                    for (FileInputSplit is : remaining) {
                        if (isLocal(host, is.getHostnames())) {
                            localSplits.add(is);
                        }
                    }
                }
                else {
                    // someone else was faster
                    localSplits = prior;
                }
            }
        }

        // at this point, we have a list of local splits (possibly empty)
        // we need to make sure no one else operates in the current list (that protects against
        // list creation races) and that the unassigned set is consistent
        // NOTE: we need to obtain the locks in this order, strictly!!!
        synchronized (localSplits) {
            int size = localSplits.size();
            if (size > 0) {
                synchronized (this.unassigned) {
                    do {
                        --size;
                        FileInputSplit split = localSplits.remove(size);
                        if (this.unassigned.remove(split)) {

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Assigning local split to host " + host);
                            }

                            localAssignments++;
                            return split;
                        }
                    } while (size > 0);
                }
            }
        }

        // we did not find a local split, return any
        synchronized (this.unassigned) {
            Iterator<FileInputSplit> iter = this.unassigned.iterator();
            if (iter.hasNext()) {
                FileInputSplit next = iter.next();
                iter.remove();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Assigning remote split to host " + host);
                }

                remoteAssignments++;
                return next;
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No more input splits remaining.");
                }
                return null;
            }
        }
    }

    private static final boolean isLocal(String host, String[] hosts) {
        if (host == null || hosts == null) {
            return false;
        }

        for (String h : hosts) {
            if (h != null && host.equals(h.toLowerCase())) {
                return true;
            }
        }

        return false;
    }

    public int getNumberOfLocalAssignments() {
        return localAssignments;
    }

    public int getNumberOfRemoteAssignments() {
        return remoteAssignments;
    }

    // -------------------------------------------------------------------------------------------

    @Override
    public InputSplit getNextInputSplit(Topology.ExecutionNode exNode) {
        return getNextInputSplit(exNode.getNodeDescriptor().getMachineDescriptor().address.getHostAddress());
    }
}