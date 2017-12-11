package com.ibm.darpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import com.ibm.disni.rdma.RdmaEndpoint;
import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvPd;
import com.ibm.disni.util.MemoryUtils;

public class DaRPCMemPoolImplSimple implements DaRPCMemPool {
//	final int messageSize;
	final int allocationSize;
	final int alignmentSize;
//	final int recvQueueLen;
//	final int sendQueueLen;
	int offset;
	ByteBuffer byteBuffer;
	IbvPd pd;
	IbvMr mr;
	int access;
	LinkedList<ByteBuffer> freeList;
	
	public DaRPCMemPoolImplSimple(int allocationSize, int alignmentSize) {
//		this.messageSize = Math.max(protocol.createRequest().size(), protocol.createResponse().size());;
		this.allocationSize = 16 * 1024 * 1024;
		this.alignmentSize = alignmentSize;
//		this.recvQueueLen = receiveQueueLength;
//		this.sendQueueLen = sendQueueLength;

		ByteBuffer rawBuffer = ByteBuffer.allocateDirect(allocationSize + alignmentSize);
		long rawBufferAddress = MemoryUtils.getAddress(rawBuffer);
		long alignmentOffset = rawBufferAddress % alignmentSize;
		if (alignmentOffset != 0) {
			rawBuffer.position(alignmentSize - (int)alignmentOffset);
		}
		byteBuffer = rawBuffer.slice();
		this.access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
	}
	
	@Override
	public void close() {
		synchronized(this) {
			try {
				mr.deregMr().execute().free();
			} catch (IOException e) {
				System.out.println("Could not unregister memory region.");
				e.printStackTrace();
			}
		}
	}

	@Override
	public ByteBuffer getBuffer(RdmaEndpoint endpoint, int size) throws IOException {
		ByteBuffer r = null;

		synchronized(this) {
			if (pd == null) {
				pd = endpoint.getPd();
			} else if (!pd.equals(endpoint.getPd())) {
				throw new IOException("No support for more than one PD");
			}
			if (mr == null) {
				mr = pd.regMr(byteBuffer, access).execute().free().getMr();
			}

			if (freeList == null) {
				int offset = size;
				freeList = new LinkedList<ByteBuffer>();
				int i = 0;
				while ((i * offset + offset) < byteBuffer.capacity()) {
					byteBuffer.position(i * offset);
					byteBuffer.limit(i * offset + offset);
					ByteBuffer b = byteBuffer.slice();
					freeList.addLast(b);
					i++;
				}
			}
			else
			{
				if (size != offset) {
					throw new IOException("Requested size does not match block size managed by memory pool.");
				}
			}
			r = freeList.getFirst();
		}
		return r; 
	}

	@Override
	public void freeBuffer(RdmaEndpoint endpoint, ByteBuffer b) {
		synchronized(this) {
			freeList.addLast(b);
		}
	}

	@Override
	public int getLKey(RdmaEndpoint endpoint, ByteBuffer b) throws IllegalArgumentException {
		return mr.getLkey();
	}
}
