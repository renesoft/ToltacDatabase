package ToltecDatabase;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import javax.lang.model.element.QualifiedNameable;

public class IndexFile {
	// public boolean m_cachedInMemory = false;
	public boolean m_isFinal = false;
	public boolean m_isSorted = true;
	public int m_fileCountElements = -1;
	/*
	 * public File m_file = null; public RandomAccessFile m_accessFile = null;
	 * 
	 * public boolean m_initSuccess = false; public long [] m_hashCache = new
	 * long[0]; public int [] m_indexCache = new int[0]; public long m_hashSize = 0;
	 */

	// For not final index
	// public long m_maxSize = 10000;
	// public ByteAbstractWorker m_byteWorker = null;

	ThreadLocal<ByteAbstractWorker> m_readWorkers = new ThreadLocal<>();
	ArrayList<ByteAbstractWorker> m_readerWorkersKeeper = new ArrayList<>();
	ByteAbstractWorker m_writeWorker = null;
	String m_fileName;

	public ByteAbstractWorker getReaderWorker() {
		ByteAbstractWorker worker = m_readWorkers.get();
		if (worker == null) {
			if (m_isFinal) {
				worker = new ByteFileWorked(m_fileName);
			} else {
				worker = new ByteFileCachedWorker(m_fileName);
			}
			// worker = new ByteFileWorked(m_fileName);
			m_readWorkers.set(worker);
			m_readerWorkersKeeper.add(worker);
		}
		return worker;
	}

	public IndexFile() {

	}
	
	public void close () {
		m_readWorkers.remove();
		for (ByteAbstractWorker byteAbstractWorker : m_readerWorkersKeeper) {
			byteAbstractWorker.close();
		}
		m_readerWorkersKeeper.clear();
		m_writeWorker.close();
	}

	public void delete() {
		new File(m_fileName).delete();
	}
	
	public IndexFile(String fileName, boolean isFinal) {
		if (isFinal == false) {
			m_isFinal = false;
			m_isSorted = false;
		} else {
			m_isFinal = true;
			m_isSorted = true;
		}
		init(fileName);
	}

	public int countElements() {
		return m_fileCountElements;
	}

	public void init(String fileName) {
		m_fileName = fileName;
		if (m_isFinal) {
			m_writeWorker = new ByteFileWorked(m_fileName);
		} else {
			m_writeWorker = new ByteFileCachedWorker(m_fileName);
		}
		m_fileCountElements = (int) (m_writeWorker.sizeBytes() / 16);
	}

	public void sort() {
		synchronized (m_writeWorker) {
			int sizeEmenents = (int) m_writeWorker.sizeBytes() / 16;
			long[] harray = new long[sizeEmenents];
			long[] iarray = new long[sizeEmenents];
			m_writeWorker.goTo(0);
			for (int i = 0; i < sizeEmenents; i++) {
				long h = m_writeWorker.readLong();
				m_writeWorker.shift(8);
				long hi = m_writeWorker.readLong();
				m_writeWorker.shift(8);
				harray[i] = h;
				iarray[i] = hi;
			}
			new Mergesort().sort(harray, iarray);
			m_writeWorker.goTo(0);
			for (int i = 0; i < sizeEmenents; i++) {
				m_writeWorker.writeLong(harray[i]);
				m_writeWorker.shift(8);
				m_writeWorker.writeLong(iarray[i]);
				m_writeWorker.shift(8);
			}
			// SortWorker.qSort(m_byteWorker, 0, countElements()-1);
			m_isSorted = true;
		}
	}

	public void add(long hash, long index) {
		synchronized (m_writeWorker) {
			if (m_isFinal) {
				System.out.println("Index is final!");
				return;
			}
			m_writeWorker.append(Primitives.LongToByteArray(hash));
			m_writeWorker.append(Primitives.LongToByteArray(index));
			m_fileCountElements++;
		}
	}

	public Pair<long[], long[]> getPositionAndIndexesByHash(long hash) {
		ByteAbstractWorker byteWorker = getReaderWorker();
		ArrayList<Long> ids = new ArrayList<>();
		ArrayList<Long> positions = new ArrayList<>();
		long pos = 0;
		byte[] arrayHash = Primitives.LongToByteArray(hash);// ByteBuffer.allocate(8).putLong(hash).array();
		if (m_isSorted) {
			// pos = BinarySearch.binarySearchInByte(m_byteWorker, arrayHash, 0,
			// countElements());
			pos = BinarySearch.binarySearchAlt(byteWorker, hash);
		}
		long len = byteWorker.sizeBytes();
		while (pos >= 0&&pos <len) {
			if (m_isSorted == false) {
				pos = BinarySearch.simpleSearchInByte(byteWorker, arrayHash, pos, countElements());
				if (pos == -1)
					break;
				else {
				}
			}
			byteWorker.goTo(pos);
			byte[] hashFromIndex = byteWorker.read(8);
			byteWorker.shift(8);
			byte[] positionFromIndex = byteWorker.read(8);
			byteWorker.shift(8);
			long readedHash = Primitives.LongFromByteArray(hashFromIndex);// ByteBuffer.wrap(hashFromIndex).getLong();
			long positionOffset = Primitives.LongFromByteArray(positionFromIndex);// ByteBuffer.wrap(positionFromIndex).getInt();
			if (readedHash == hash ) {
				if (positionOffset!=-1L) {
					ids.add(positionOffset);
					positions.add(pos);
				}else {
					//continue;	
				}
			}
			pos += 16;
		}
		long[] retOffsets = new long[ids.size()];
		Iterator<Long> iterator = ids.iterator();
		for (int i = 0; i < retOffsets.length; i++) {
			retOffsets[i] = iterator.next().longValue();
		}
		long[] retPositions = new long[positions.size()];
		Iterator<Long> iterator2 = positions.iterator();
		for (int i = 0; i < retPositions.length; i++) {
			retPositions[i] = iterator2.next().longValue();
		}
		return new Pair<long[], long[]>(retPositions, retOffsets);
	}

	public long[] getIndexesByHash(long hash) { // return offsets in record in database
		Pair<long[], long[]> pair = getPositionAndIndexesByHash(hash);
		return pair.getValue();
	}

	/*public void nullRecords(long offset) {
		synchronized (m_writeWorker) {
			dumpData("nullRecords Before:", offset, 2);
			m_writeWorker.goTo(offset);
			m_writeWorker.writeLong(0);
			m_writeWorker.shift(8);
			m_writeWorker.writeLong(0);
			dumpData("nullRecords After :", offset, 2);
		}

	}*/

	public void updatePosition(long offset, long newPosition) {
		synchronized (m_writeWorker) {
			m_writeWorker.goTo(offset + 8);
			m_writeWorker.writeLong(newPosition);
		}
	}

	public void dumpData(String label, long offset, int count) {
		ByteAbstractWorker byteWorker = getReaderWorker();
		if (label != null && label.length() > 0)
			System.out.print(label + ":");
		byteWorker.goTo(offset - (count * 16));
		for (int i = -count; i < count; i++) {
			if (i == 0)
				System.out.print(" [");
			System.out.print(byteWorker.readLongShift());
			System.out.print(" ");
			System.out.print(byteWorker.readLongShift());
			if (i == 0)
				System.out.print("] ");
			System.out.print("|");
		}
		System.out.println("");
	}

	public void dump(String label) {
		ByteAbstractWorker byteWorker = getReaderWorker();
		byteWorker.goTo(0);
		while (true) {
			byte[] a = byteWorker.read(8);
			if (a == null)
				break;
			byteWorker.shift(8);
			byte[] b = byteWorker.read(8);
			if (b == null)
				break;
			byteWorker.shift(8);

			for (int i = 0; i < a.length; i++) {
				System.out.print("" + a[i] + " ");
			}
			System.out.print("|");
			for (int i = 0; i < b.length; i++) {
				System.out.print("" + b[i] + " ");
			}
			System.out.println("");
		}
	}
	public void updateHashInPosition (long position, long hash, long dataPosition) {
		//long checkHash = Primitives.LongFromByteArray(m_writeWorker.readInPos(position, 8));
		m_writeWorker.goTo(position);
		m_writeWorker.writeLong(hash);
		//long checkHash2 = Primitives.LongFromByteArray(m_writeWorker.readInPos(position, 8));
		m_writeWorker.shift(8);
		m_writeWorker.writeLong(dataPosition);
				
	}
	
	public void cleanHashInPostion (long position) {
		m_writeWorker.goTo(position+8);
		m_writeWorker.writeLong(-1L);
	}
	
	
	public int updateHash(long oldHash, long newHash, long oldPos, long newPos) {
		synchronized (m_writeWorker) {
			Pair<long[], long[]> pair = getPositionAndIndexesByHash(oldHash);
			long[] pos = pair.getKey();
			long[] posOffsets = pair.getValue();
						
			if (oldHash == newHash) {
				for (int i = 0; i < pos.length; i++) {
					if (posOffsets[i] != oldPos)
						continue;
					updateHashInPosition(pos[i],newHash,newPos);
				}
			}else {
				for (int i = 0; i < pos.length; i++) {
					if (posOffsets[i] != oldPos)
						continue;
					long l1 = m_writeWorker.sizeBytes();
					cleanHashInPostion(pos[i]);
					long l2 = m_writeWorker.sizeBytes();
					//System.out.println("123");
				}				
			}
			/*for (int i = 0; i < pos.length; i++) {
				if (posOffsets[i] != oldPos)
					continue;
				if (oldHash == newHash) {

					long checkHash = Primitives.LongFromByteArray(m_writeWorker.readInPos(pos[i], 8));

					m_writeWorker.goTo(pos[i]);
					m_writeWorker.writeLong(newHash);
					long checkHash2 = Primitives.LongFromByteArray(m_writeWorker.readInPos(pos[i], 8));
					m_writeWorker.shift(8);
					if (newPos != 0) {
						m_writeWorker.writeLong(newPos);
					}
				} else {
					m_writeWorker.goTo(pos[i] + 8);
					m_writeWorker.writeLong(newPos);
				}
			}*/
			return 0;
		}
	}

	void analize() {
		ByteAbstractWorker byteWorker = getReaderWorker();
		if (!m_isSorted) {
			System.out.println("Can't analize unsorted array.");
			return;
		}
		int sizeEmenents = (int) byteWorker.sizeBytes() / 16;
		byteWorker.goTo(0);
		long lastHash = 0;
		for (int i = 0; i < sizeEmenents; i++) {
			long h = byteWorker.readLong();
			if (i == 0) {
				lastHash = h;
				continue;
			}
			if (h < lastHash) {
				System.out.println("Error at pos:" + i + " hash " + h + "less that " + lastHash);
			}
			byteWorker.shift(8);
			long hi = byteWorker.readLong();
			byteWorker.shift(8);
			// harray[i]=h;
			// iarray[i]=hi;
		}
	}

	public static void main(String[] args) {

	}

}
