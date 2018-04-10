package org.apache.crail.namenode;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.utils.AtomicIntegerModulo;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by atr on 10.04.18.
 */
public class PocketBlockStore {
    private static final Logger LOG = CrailUtils.getLogger();

    // this is the top level indexing based upon the storage classes like NVMe or DRAM
    private PocketStorageClass[] storageClasses;

    PocketBlockStore() {
        storageClasses = new PocketStorageClass[CrailConstants.STORAGE_CLASSES];
        for (int i = 0; i < CrailConstants.STORAGE_CLASSES; i++) {
            this.storageClasses[i] = new PocketStorageClass(i);
        }
    }

    short removeDN(DataNodeInfo dn) throws Exception {
        int storageClass = dn.getStorageClass();
        return storageClasses[storageClass].removeDatanode(dn);
    }

    short addBlock(NameNodeBlockInfo blockInfo) throws Exception {
        int storageClass = blockInfo.getDnInfo().getStorageClass();
        return storageClasses[storageClass].addBlock(blockInfo);
    }

    private NameNodeBlockInfo _getBlock(int storageClass, int locationAffinity) throws InterruptedException {
        NameNodeBlockInfo block = null;
        if (storageClass > 0) {
            if (storageClass < storageClasses.length) {
                block = storageClasses[storageClass].getBlock(locationAffinity);
            } else {
                //TODO: warn if requested storage class is invalid
            }
        }
        if (block == null) {
            for (int i = 0; i < storageClasses.length; i++) {
                block = storageClasses[i].getBlock(locationAffinity);
                if (block != null) {
                    break;
                }
            }
        }

        return block;
    }

    public NameNodeBlockInfo getBlock(int storageClass, int locationAffinity) throws InterruptedException {
        boolean found = false;
        NameNodeBlockInfo block = null;
        while (!found) {
            block = _getBlock(storageClass, locationAffinity);
            if (block != null && !block.isDeleted())
                found = true;
        }
        return block;
    }

    DataNodeBlocks getDataNode(DataNodeInfo dnInfo) {
        int storageClass = dnInfo.getStorageClass();
        return storageClasses[storageClass].getDataNode(dnInfo);
    }
}

class PocketStorageClass {
    private static final Logger LOG = CrailUtils.getLogger();
    private int storageClass;
    private ConcurrentIndexedHashMap membership;
    private BlockSelection blockSelection;

    PocketStorageClass(int storageClass) {
        this.storageClass = storageClass;
        this.membership = new ConcurrentIndexedHashMap();
        if (CrailConstants.NAMENODE_BLOCKSELECTION.equalsIgnoreCase("roundrobin")) {
            this.blockSelection = new RoundRobinBlockSelection();
        } else {
            this.blockSelection = new RandomBlockSelection();
        }
    }

    short addBlock(NameNodeBlockInfo block) throws Exception {
        DataNodeBlocks current = membership.getByDataNode(block.getDnInfo());
        if (current == null) {
            // we need to insert a new one
            current = DataNodeBlocks.fromDataNodeInfo(block.getDnInfo());
            this.membership.add(current);
            LOG.info("adding datanode " + CrailUtils.getIPAddressFromBytes(current.getIpAddress()) + ":" + current.getPort() + " of type " + current.getStorageType() + " to storage class " + storageClass);
        }
        current.touch();
        current.addFreeBlock(block);
        return RpcErrors.ERR_OK;
    }

    short removeDatanode(DataNodeInfo dn) throws Exception {
        DataNodeBlocks old = membership.remove(dn);
        if (old == null) {
            System.err.println("DataNode: " + dn.toString() + " not found");
            return RpcErrors.ERR_DATANODE_NOT_REGISTERED;
        } else {
            System.err.println("DataNode: " + dn.toString() + " scheduled for removal from the list");
            return RpcErrors.ERR_OK;
        }
    }

    NameNodeBlockInfo getBlock(int affinity) throws InterruptedException {
        NameNodeBlockInfo block = null;
        int size = this.membership.size();
        if (size > 0) {
            int startIndex = blockSelection.getNext(size);
            for (int i = 0; i < size; i++) {
                int index = (startIndex + i) % size;
                DataNodeBlocks anyDn = this.membership.getByIndex(index);
                if (anyDn.isOnline()) {
                    block = anyDn.getFreeBlock();
                }
                if (block != null) {
                    break;
                }
            }
        }
        return block;
    }

    DataNodeBlocks getDataNode(DataNodeInfo dataNode) {
        return membership.getByDataNode(dataNode);
    }

    public interface BlockSelection {
        int getNext(int size);
    }

    private class RoundRobinBlockSelection implements BlockSelection {
        private AtomicIntegerModulo counter;

        RoundRobinBlockSelection() {
            LOG.info("round robin block selection");
            counter = new AtomicIntegerModulo();
        }

        @Override
        public int getNext(int size) {
            return counter.getAndIncrement() % size;
        }
    }

    private class RandomBlockSelection implements BlockSelection {
        RandomBlockSelection() {
            LOG.info("random block selection");
        }

        @Override
        public int getNext(int size) {
            return ThreadLocalRandom.current().nextInt(size);
        }
    }

    private class ConcurrentIndexedHashMap {
        // this is the hashmap from long -> DataNodeBlocks
        private HashMap<Long, DataNodeBlocks> membership;
        // then we need int index -> long map
        private ArrayList<Long> indexToLong;
        private ReentrantReadWriteLock lock;

        ConcurrentIndexedHashMap() {
            this.indexToLong = new ArrayList<>();
            this.membership = new HashMap<>();
            this.lock = new ReentrantReadWriteLock();
        }

        private void sanityCheck() throws Exception{
            if(this.membership.size() != this.indexToLong.size()){
                LOG.error(" Something went wrong in insertion ");
                throw new Exception();
            }
        }

        int size() {
            return this.membership.size();
        }

        void add(DataNodeBlocks dataNode) throws Exception{
            lock.writeLock().lock();
            try {
                boolean isThere = this.membership.containsKey(dataNode.key());
                if(!isThere){
                    this.membership.put(dataNode.key(), dataNode);
                    // this will give it an index
                    this.indexToLong.add(dataNode.key());
                }// otherwise we don't have to do anything
            } finally {
                sanityCheck();
                lock.writeLock().unlock();
            }
        }

        DataNodeBlocks remove(DataNodeInfo dataNode) throws Exception {
            DataNodeBlocks retValue = null;
            lock.writeLock().lock();
            try {
                boolean isThere = this.membership.containsKey(dataNode.key());
                if(isThere){
                    // if it was in there then remove it from both data strucutres
                    retValue = this.membership.remove(dataNode.key());
                    this.indexToLong.remove(dataNode.key());
                }else {
                    retValue = null;
                    LOG.error("bad stuff");
                }
            } finally {
                sanityCheck();
                lock.writeLock().unlock();
            }
            return retValue;
        }

        DataNodeBlocks getByIndex(int index) {
            lock.readLock().lock();
            try {
                return this.membership.get(this.indexToLong.get(index));
            } finally {
                lock.readLock().unlock();
            }
        }

        DataNodeBlocks getByDataNode(DataNodeInfo dn) {
            lock.readLock().lock();
            try {
                return this.membership.get(dn.key());
                // something
            } finally {
                lock.readLock().unlock();
            }
        }
    }
}