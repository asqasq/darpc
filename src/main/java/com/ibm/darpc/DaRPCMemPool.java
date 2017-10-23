package com.ibm.darpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import com.ibm.disni.rdma.RdmaEndpoint;
import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvPd;
import com.ibm.disni.util.MemoryUtils;

public abstract class DaRPCMemPool {
	final int defaultAllocationSize = 16 * 1024 * 1024; // 16MB
	final int defaultMinAllocationSize = 4 * 1024; // 4KB
	final int defaultAlignmentSize = 4 * 1024; // 4KB

	private HashMap<IbvPd, PdMemPool> pdMemPool; // One buddy allocator per protection domain
	private LinkedList<IbvMr> mrs;
	private boolean isOpen;

	// Configurable values
	protected int allocationSize;
	protected int minAllocationSize;
	protected int alignmentSize;

	protected int access; // RDMA access flags to use when registering memory regions


	// This method must be implemented by subclasses
	protected abstract ByteBuffer allocateBuffer() throws IOException;



	public DaRPCMemPool() {
		isOpen = false;
		pdMemPool = new HashMap<IbvPd, PdMemPool>();
		mrs = new LinkedList<IbvMr>();
		allocationSize = defaultAllocationSize;
		minAllocationSize = defaultMinAllocationSize;
		alignmentSize = defaultAlignmentSize;
		this.access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
		isOpen = true;
	}

	public void init(int allocationSize, int minAllocationSize, int alignmentSize) {
		if (allocationSize >= 0) {
			this.allocationSize = allocationSize;
		}
		if (minAllocationSize >= 0) {
			this.minAllocationSize = minAllocationSize;
		}
		if (alignmentSize >= 0) {
			this.alignmentSize = alignmentSize;
		}
	}

	public synchronized void close() {
		if (isOpen) {
			isOpen = false;
			pdMemPool = null;
			for (Iterator<IbvMr> it = mrs.iterator(); it.hasNext(); ) {
				IbvMr m = it.next();
				try {
					m.deregMr().execute().free();
				} catch (IOException e) {
					System.out.println("Could not unregister memory region.");
					e.printStackTrace();
				}
			}
			mrs = null;
		}
	}

	public void finalize() {
		// Just in case the user did not do that.
		close();
	}


	// Add a new chunk and register it with the IB device.
	// This adds a new "root" to the buddy tree.
	protected void addNewBuddy(PdMemPool pdm) throws IOException {
		BuddyInfo bi = new BuddyInfo();

		bi.buffer = allocateBuffer();
		// Register buffer with IB card
		IbvMr mr = pdm.pd.regMr(bi.buffer, access).execute().free().getMr();
		mrs.addLast(mr);

		bi.s = state.FREE;
		bi.size = allocationSize;
		bi.parent = null;
		bi.sibling = null;
		bi.lkey = mr.getLkey();

		if (pdm.freeBuddies.get(allocationSize) == null) {
			pdm.freeBuddies.put(allocationSize, new LinkedList<BuddyInfo>());
		}
		pdm.freeBuddies.get(allocationSize).add(bi);
	}

	protected boolean split(PdMemPool pdm, int size) {
		if (size > allocationSize) {
			return false;
		}
		if (!pdm.freeBuddies.containsKey(size)) {
			if (!split(pdm, size << 1)) {
				// no free buddy, which could be split
				return false;
			}
		}
		LinkedList<BuddyInfo> l = pdm.freeBuddies.get(size);
		if (l == null) {
			return false;
		}
		BuddyInfo bi = l.removeFirst();
		if (l.size() == 0) {
			pdm.freeBuddies.remove(size);
		}
		bi.s = state.SPLIT;
		bi.buffer.position(0);
		bi.buffer.limit(size >> 1);
		ByteBuffer b1 = bi.buffer.slice();
		bi.buffer.position(size >> 1);
		bi.buffer.limit(size);
		ByteBuffer b2 = bi.buffer.slice();

		BuddyInfo bi1 = new BuddyInfo();
		BuddyInfo bi2 = new BuddyInfo();
		bi1.buffer = b1;
		bi1.s = state.FREE;
		bi1.size = (size >> 1);
		bi1.parent = bi;
		bi1.sibling = bi2;
		bi1.lkey = bi.lkey;

		bi2.buffer = b2;
		bi2.s = state.FREE;
		bi2.size = (size >> 1);
		bi2.parent = bi;
		bi2.sibling = bi1;
		bi2.lkey = bi.lkey;

		if (pdm.freeBuddies.get(size >> 1) == null) {
			pdm.freeBuddies.put(size >> 1, new LinkedList<BuddyInfo>());
		}
		pdm.freeBuddies.get(size >> 1).add(bi1);
		pdm.freeBuddies.get(size >> 1).add(bi2);

		return true;
	}


	protected ByteBuffer getPower2Buffer(PdMemPool pdm, int size) {
		if (!pdm.freeBuddies.containsKey(size)) {
			if (!split(pdm, size << 1)) {
				// no free buddy, which could be split
				return null;
			}
		}
		LinkedList<BuddyInfo> l = pdm.freeBuddies.get(size);
		if (l == null) {
			return null;
		}
		BuddyInfo bi = l.removeFirst();
		if (l.size() == 0) {
			pdm.freeBuddies.remove(size);
		}
		bi.s = state.USED;
		pdm.usedBuddies.put(MemoryUtils.getAddress(bi.buffer), bi);
		return bi.buffer;
	}

	synchronized ByteBuffer getBuffer(RdmaEndpoint endpoint, int size) throws IOException {
		int i = minAllocationSize;

		if (!pdMemPool.containsKey(endpoint.getPd())) {
			pdMemPool.put(endpoint.getPd(), new PdMemPool(endpoint.getPd()));
		}
		PdMemPool pdm = pdMemPool.get(endpoint.getPd());

		while(size > i) {
			i <<= 1;
		}

		ByteBuffer b = getPower2Buffer(pdm, i);
		if (b == null) {
			addNewBuddy(pdm);
			b = getPower2Buffer(pdm, i);
		}
		return (b);
	}

	protected void merge(PdMemPool pdm, BuddyInfo bi) {
		if (bi.sibling != null) {
			if (bi.sibling.s == state.FREE) {
				BuddyInfo parent = bi.parent;
				parent.s = state.FREE;
				if (pdm.freeBuddies.get(parent.size) == null) {
					pdm.freeBuddies.put(parent.size, new LinkedList<BuddyInfo>());
				}
				pdm.freeBuddies.get(parent.size).add(parent);
				pdm.freeBuddies.get(bi.size).remove(bi.sibling);
				pdm.freeBuddies.get(bi.size).remove(bi);
				if (pdm.freeBuddies.get(bi.size).size() == 0) {
					pdm.freeBuddies.remove(bi.size);
				}
				merge(pdm, parent);
			}
		}
	}
	synchronized void freeBuffer(RdmaEndpoint endpoint, ByteBuffer b) {
		if (b == null) {
			return;
		}
		PdMemPool pdm = pdMemPool.get(endpoint.getPd());
		BuddyInfo bi = pdm.usedBuddies.remove(MemoryUtils.getAddress(b));
		// Buffer is not in the used list. Cannot free.
		if (bi == null) {
			return;
		}
		bi.s = state.FREE;
		if (pdm.freeBuddies.get(bi.size) == null) {
			pdm.freeBuddies.put(bi.size, new LinkedList<BuddyInfo>());
		}
		pdm.freeBuddies.get(bi.size).add(bi);
		merge(pdm, bi);
	}

	int getLKey(RdmaEndpoint endpoint, ByteBuffer b) throws IllegalArgumentException {
		if (b == null) {
			System.out.println("getLKey(): Argument buffer is null. Cannot return lkey.");
			throw new IllegalArgumentException("getLKey(): Argument buffer is null. Cannot return lkey.");
		}
		PdMemPool pdm = pdMemPool.get(endpoint.getPd());
		BuddyInfo bi = pdm.usedBuddies.get(MemoryUtils.getAddress(b));
		if (bi != null) {
			return bi.lkey;
		} else {
			System.out.println("getLKey(): This buffer is not allocated. Cannot return lkey.");
			throw new IllegalArgumentException("getLKey(): This buffer is not allocated. Cannot return lkey.");
		}
	}

	void setAllocationSize(int size) {
		allocationSize = size;
	}

	void setMinAllocationSize(int size) {
		minAllocationSize = size;
	}

	void setAlignment(int size) {
		alignmentSize = size;
	}

	int getAllocationSize() {
		return allocationSize;
	}

	int getMinAllocationSize(int size) {
		return minAllocationSize;
	}

	int getAlignment(int size) {
		return alignmentSize;
	}

	void printBuddies() {
		System.out.println("Free buddies:\n============");
		for (Iterator<PdMemPool> itpd = pdMemPool.values().iterator(); itpd.hasNext(); ) {
			PdMemPool pdm = itpd.next();
			for (Iterator<Integer> it = pdm.freeBuddies.keySet().iterator(); it.hasNext(); ) {
				Integer size = it.next();
				System.out.println("Size: " + size);
				LinkedList<BuddyInfo> l = pdm.freeBuddies.get(size);
				if (l != null) {
					for (Iterator<BuddyInfo> it2 = l.iterator(); it2.hasNext(); ) {
						BuddyInfo bi = it2.next();
						System.out.println(bi);
					}
				}
			}
		}
		System.out.println("============\n");

		System.out.println("Used buddies:\n============");
		for (Iterator<PdMemPool> itpd = pdMemPool.values().iterator(); itpd.hasNext(); ) {
			PdMemPool pdm = itpd.next();
			for (Iterator<BuddyInfo> it = pdm.usedBuddies.values().iterator(); it.hasNext(); ) {
				System.out.println(it.next());
			}
		}
		System.out.println("============\n");
	}



	// Internally used
	enum state {
		FREE,
		USED,
		SPLIT
	}

	// Internally used to keep track of buffer state
	class BuddyInfo {
		ByteBuffer buffer;
		BuddyInfo parent;
		BuddyInfo sibling;
		state s;
		int size;
		int lkey;
		public String toString() {
			return new String("Size= " + size + ", state = "
					+ (s == state.FREE ? "FREE": s == state.USED ? "USED" : "SPLIT")
					+ ", address = " + MemoryUtils.getAddress(buffer)
					+ ", capacity = " + buffer.capacity()
					+ ", lkey = " + lkey);
		}
	}

	class PdMemPool {
		HashMap<Integer, LinkedList<BuddyInfo>> freeBuddies;
		HashMap<Long, BuddyInfo> usedBuddies;
		IbvPd pd;

		PdMemPool(IbvPd pd) {
			this.pd = pd;
			freeBuddies = new HashMap<Integer, LinkedList<BuddyInfo>>();
			usedBuddies = new HashMap<Long, BuddyInfo>();
		}
	}
}
