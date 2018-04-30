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

import Tools.Pair;

public class IndexFileMaped extends IndexFile {

	HashMap<Long, ArrayList<Long>> m_mapedHashes = new HashMap<>();
	HashMap<Long, ArrayList<Integer>> m_mapedPositions = new HashMap<>();

	public IndexFileMaped() {

	}

	public IndexFileMaped(String fileName) {
		init(fileName);
	}

	public void addMap(long hash, long pos, int index) {
		ArrayList<Long> poss = m_mapedHashes.get(hash);
		ArrayList<Integer> idxs = m_mapedPositions.get(hash);
		if (poss == null) {
			poss = new ArrayList<>();
			idxs = new ArrayList<>();
			m_mapedHashes.put(hash, poss);
			m_mapedPositions.put(hash, idxs);
		}
		poss.add(pos);
		idxs.add(index);
	}

	public IndexFileMaped(String fileName, boolean isFinal) {

		m_isFinal = false;
		m_isSorted = false;

		init(fileName);
	}

	public int countElements() {
		return m_fileCountElements;
	}

	public void init(String fileName) {
		m_fileName = fileName;
		m_writeWorker = new ByteFileWorked(fileName);
		long check = m_writeWorker.sizeBytes() % 16;
		if (check != 0) {
			System.out.println("Index broken!");
		} else {
			System.out.println("Index fine.");
		}
		m_fileCountElements = (int) (m_writeWorker.sizeBytes() / 16);
		for (int i = 0; i < m_fileCountElements; i++) {
			addMap(m_writeWorker.readLongShift(), m_writeWorker.readLongShift(), i);
		}

	}

	public void add(long hash, long index) {
		super.add(hash, index);
		addMap(hash, index, m_fileCountElements - 1);

	}

	public Pair<long[], long[]> getPositionAndIndexesByHash(long hash) {

		ArrayList<Long> ids = m_mapedHashes.get(hash);
		ArrayList<Long> positions = new ArrayList<>();
		ArrayList<Integer> ipositions = m_mapedPositions.get(hash);
		if (ids == null) {
			ids = new ArrayList<>();
			ipositions = new ArrayList<>();
		}
		for (Integer int1 : ipositions) {
			positions.add((long) (int1 * 16));
		}

		long[] retOffsets = new long[ids.size()];
		Iterator<Long> iterator = ids.iterator();
		for (int i = 0; i < retOffsets.length; i++) {
			long l = iterator.next().longValue();
			retOffsets[i] = l;
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

	public void dump(String label) {
		synchronized (m_writeWorker) {
			m_writeWorker.goTo(0);
			while (true) {
				byte[] a = m_writeWorker.read(8);
				if (a == null)
					break;
				m_writeWorker.shift(8);
				byte[] b = m_writeWorker.read(8);
				if (b == null)
					break;
				m_writeWorker.shift(8);

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
	}

	public int updateHash(long oldHash, long newHash, long oldPos, long newPos) {
		super.updateHash(oldHash, newHash, oldPos, newPos);
		ArrayList<Long> ids = m_mapedHashes.get(oldHash);
		if (ids==null)
			return -1;
		ArrayList<Integer> ipositions = m_mapedPositions.get(oldHash);
		//ArrayList<Long> Newids = m_mapedHashes.get(newHash);
		//ArrayList<Integer> Newipositions = m_mapedPositions.get(newHash);		
		if (oldHash == newHash) {			
				for (int i = 0; i < ids.size(); i++) {
					if (ids.get(i) == oldPos) {
						ids.remove(i);
						ids.add(i, newPos);
						break ;
					}
				}
		}else {
			for (int i = 0; i < ids.size(); i++) {
				if (ids.get(i) == oldPos) {
					ids.remove(i);
					ipositions.remove(i);
					break ;
				}
			}
			add(newHash,newPos);
		}
	
		return 0;
	}
}
