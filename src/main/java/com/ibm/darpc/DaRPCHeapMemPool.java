package com.ibm.darpc;

import java.nio.ByteBuffer;

public class DaRPCHeapMemPool extends DaRPCMemPool {
	protected ByteBuffer allocateBuffer() {
		ByteBuffer byteBuffer;

		if (alignmentSize > 1) {
			ByteBuffer rawBuffer = ByteBuffer.allocateDirect(allocationSize + alignmentSize);
			long rawBufferAddress = ((sun.nio.ch.DirectBuffer)rawBuffer).address();
			long alignmentOffset = rawBufferAddress % alignmentSize;
			if (alignmentOffset != 0) {
				rawBuffer.position(alignmentSize - (int)alignmentOffset);
			}
			byteBuffer = rawBuffer.slice();

		} else {
			byteBuffer = ByteBuffer.allocateDirect(allocationSize);
		}
		return (byteBuffer);
	}
}
