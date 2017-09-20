package com.ibm.darpc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import com.ibm.disni.rdma.verbs.IbvPd;

public class DaRPCHugePagesMemPool extends DaRPCMemPool {
	long currentRegion = 0;
	String directory = "/tmp/darpccache";
	private File dir;


	public DaRPCHugePagesMemPool() {
		super();
		dir = new File(directory);
		if (!dir.exists()){
			dir.mkdirs();
		}
		for (File child : dir.listFiles()) {
			child.delete();
		}
	}

	public void init(String directory) {
		super.init();
		this.directory = directory;
	}

	protected ByteBuffer allocateBuffer() throws IOException {
		String path = directory + "/" + currentRegion++ + ".mem";
		RandomAccessFile randomFile = null;
		try {
			randomFile = new RandomAccessFile(path, "rw");
		} catch (FileNotFoundException e) {
			System.out.println("Path " + path + " to huge page directory not found.");
			throw e;
		}
		try {
			randomFile.setLength(allocationSize);
		} catch (IOException e) {
			System.out.println("Coult not set allocation length of mapped random access file on huge page directory.");
			randomFile.close();
			throw e;
		}
		FileChannel channel = randomFile.getChannel();
		MappedByteBuffer mappedBuffer = null;
		try {
			mappedBuffer = channel.map(MapMode.READ_WRITE, 0,
					allocationSize);
		} catch (IOException e) {
			System.out.println("Could not map the huge page file on path " + path);
			randomFile.close();
			throw e;
		}
		randomFile.close();
		return (mappedBuffer);
	}

	public synchronized void close() {
		super.close();
		if (dir.exists()){
			for (File child : dir.listFiles()) {
				child.delete();
			}
			dir.delete();
		}
	}
}
