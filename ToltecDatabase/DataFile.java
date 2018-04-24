package ToltecDatabase;

import java.io.File;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import Tools.MurmurHash;
import Tools.ThreadLocalManaged;

public class DataFile extends Thread {

	public static class WriteQuery {
		public static int OP_ADD = 0;
		public static int OP_UPDATE = 1;
		public static int OP_DELETE = 2;
		DataRow data;
		String whereCols;
		Object whereValue;
		int operation = 0;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DataFile file = new DataFile("C:/tmp/first.database", false);
		file.addColumn("id", TableSchema.TYPE_INT, false, false);
		file.addColumn("name", TableSchema.TYPE_STRING, false, false);
		file.addColumn("hash", TableSchema.TYPE_LONG, false, false);

		file.databaseDump("");

		DataRow row = new DataRow();
		row.put("id", 1);
		row.put("name", "Aleksey Abelar");
		row.put("hash", 9223000000000000000L);
		file.addData(row);
		row.put("id", 2);
		row.put("name", "Abstracto Nagua");
		row.put("hash", 1223000000000000000L);
		file.addData(row);

	}

	// File format [ 4 byte - size of all data block] [ [data 1 - N bytes] [data 2 -
	// N bytes] ... [data K - N bytes] ]
	public boolean m_isFinal = false;
	public TableSchema m_schema = new TableSchema();
	String m_fileName;
	// String m_indexFolder;
	String m_indexFileName;
	// ByteAbstractWorker m_fileWorker ;
	//ThreadLocal<ByteAbstractWorker> m_readWorkers = new ThreadLocal<>();
	ThreadLocalManaged<ByteAbstractWorker> m_readWorkers = new ThreadLocalManaged<>();
	
	ByteAbstractWorker m_writeWorker = null;
	// ByteAbstractWorker m_writeWorkerUpdate = null;

	public ByteAbstractWorker getReaderWorker() {
		ByteAbstractWorker worker = m_readWorkers.get();
		if (worker == null) {
			worker = new ByteFileWorked(m_fileName);
			m_readWorkers.set(worker);
		}
		return worker;
	}

	public void removeReaderWorker() {		
		m_readWorkers.removeAll(new ThreadLocalManaged.ThreadLocalManagedRemoveIterator<ByteAbstractWorker>() {
			@Override
			public void removed(ByteAbstractWorker t) {
				t.close();				
			}
		});
		
		/*ByteAbstractWorker baw = m_readWorkers.get();
		if (baw!=null)
			baw.close();
		m_readWorkers.remove();*/
	}

	
	public void close () {
		if (m_writeWorker!=null)
			m_writeWorker.close();
		removeReaderWorker();				
	}
	
	public void remove () {
		new File (m_fileName).delete();
	}
	
	public void rename (String name) {
		File f = new File (m_fileName);		
		f.renameTo(new File (f.getParentFile(),name));
	}
	
	// ByteAbstractWorker m_positionFileWorker ;
	public boolean addColumn(String name, int type, boolean isIndex, boolean isSorted) {
		m_schema.m_tableTypesWithNames.put(name, type);
		// m_schema.m_tableNamesToHash.put(name, name.hashCode());
		if (isIndex) {
			IndexManager index = new IndexManager(m_indexFileName + "." + name + ".index");
			m_schema.m_columnsToIndexFilePrefix.put(name, index);
		}
		return true;
	}

	public IndexManager getIndex(String colName) {
		return m_schema.m_columnsToIndexFilePrefix.get(colName);
	}

	public DataFile(String fileName, String indexFolder, boolean isFinal) {
		if (isFinal == false) {
			m_isFinal = false;
		} else {
			m_isFinal = true;
		}
		init(fileName, indexFolder);
	}

	public DataFile(String fileName, boolean isFinal) {
		if (isFinal == false) {
			m_isFinal = false;
		} else {
			m_isFinal = true;
		}
		init(fileName);
	}

	public void init(String fileName) {
		init(fileName, null);
	}

	public void init(String fileName, String indexFolder) {
		m_fileName = fileName;
		m_indexFileName = m_fileName;
		if (indexFolder != null) {
			String name = new File(m_fileName).getName();
			m_indexFileName = new File(indexFolder, name).getAbsolutePath();
		}
		m_writeWorker = new ByteFileWorked(m_fileName);
		// m_writeWorkerUpdate = new ByteFileWorked(m_fileName);
		// m_fileWorker = new ByteFileWorked(fileName);
		// m_positionFileWorker = new ByteFileWorked(fileName+".positions");
	}

	long objectToHash(int type, Object obj) throws Exception {
		byte[] buf = objectToByteArray(type, obj);
		long hash = MurmurHash.hash64(buf, buf.length);
		return hash;
	}

	byte[] objectToByteArray(int type, Object obj) throws Exception {
		ExtendableByteArray eba = new ExtendableByteArray(8);
		if (type == TableSchema.TYPE_INT) {
			if (obj == null)
				obj = new Integer(0);
			return Primitives.IntToByteArray((Integer) obj);
			// return ByteBuffer.allocate(4).putInt((Integer)obj).array();
		}
		if (type == TableSchema.TYPE_LONG) {
			if (obj == null)
				obj = new Long(0);
			return Primitives.LongToByteArray((Long) obj);
			// return ByteBuffer.allocate(8).putLong((Long)obj).array();
		}
		if (type == TableSchema.TYPE_FLOAT) {
			if (obj == null)
				obj = new Float(0);
			Primitives.FloatToByteArray((Float) obj);
			// return ByteBuffer.allocate(4).putFloat((Float)obj).array();
		}
		if (type == TableSchema.TYPE_DOUBLE) {
			if (obj == null)
				obj = new Double(0);
			Primitives.DoubleToByteArray((Double) obj);
			// return ByteBuffer.allocate(8).putDouble((Double)obj).array();
		}
		if (type == TableSchema.TYPE_STRING) {
			if (obj == null)
				obj = new String();
			String s = (String) obj;
			return s.getBytes("UTF-8");
		}
		if (type == TableSchema.TYPE_BYTEARRAY) {
			if (obj == null)
				obj = new byte[0];
			return (byte[]) obj;
		}
		if (type == TableSchema.TYPE_OBJECT) {
			// TODO: need add Kryo lib
		}
		return eba.bufferCopyWithTrim();
	}

	Object byteArrayToObject(int type, byte[] array) throws Exception {
		return byteArrayToObject(type, array, 0, array.length);
	}

	Object byteArrayToObject(int type, byte[] array, int offset, int len) throws Exception {
		if (type == TableSchema.TYPE_INT) {
			return Primitives.IntFromByteArray(array, offset);
			// return ByteBuffer.wrap(array,offset,len).getInt();
		}
		if (type == TableSchema.TYPE_LONG) {
			return Primitives.LongFromByteArray(array, offset);
			// return ByteBuffer.wrap(array,offset,len).getLong();
		}
		if (type == TableSchema.TYPE_FLOAT) {
			return Primitives.FloatFromByteArray(array, offset);
			// return ByteBuffer.wrap(array,offset,len).getFloat();
		}
		if (type == TableSchema.TYPE_DOUBLE) {
			return Primitives.DoubleFromByteArray(array, offset);
			// return ByteBuffer.wrap(array,offset,len).getDouble();
		}
		if (type == TableSchema.TYPE_STRING) {
			return new String(array, offset, len, "UTF-8");
		}
		if (type == TableSchema.TYPE_BYTEARRAY) {
			return Arrays.copyOfRange(array, offset, offset + len);
		}
		if (type == TableSchema.TYPE_OBJECT) {
			// TODO: need add Kryo lib
		}
		return null;
	}

	/*
	 * int byteArrayToInt (byte [] array){ return
	 * byteArrayToInt(array,0,array.length); }
	 * 
	 * int byteArrayToInt (byte [] array, int offset, int len){ return
	 * ByteBuffer.wrap(array,offset,len).getInt(); }
	 */

	public long addDataByTextFormat(String[] data) {
		DataRow map = new DataRow();
		for (String string : data) {
			String[] pair = string.split("=", 2);
			if (pair.length != 2) {
				return -1;
			}
			int type = m_schema.m_tableTypesWithNames.get(pair[0]);
			if (type == TableSchema.TYPE_INT) {
				map.put(pair[0], Integer.parseInt(pair[1]));
			}
			if (type == TableSchema.TYPE_LONG) {
				map.put(pair[0], Long.parseLong(pair[1]));
			}
			if (type == TableSchema.TYPE_FLOAT) {
				map.put(pair[0], Float.parseFloat(pair[1]));
			}
			if (type == TableSchema.TYPE_DOUBLE) {
				map.put(pair[0], Double.parseDouble(pair[1]));
			}
			if (type == TableSchema.TYPE_STRING) {
				map.put(pair[0], pair[1]);
			}
		}
		return addData(map);

	}

	byte[] buildRow(DataRow data) {
		ExtendableByteArray eba = new ExtendableByteArray();
		for (String colName : m_schema.m_tableTypesWithNames.keySet()) {
			int type = m_schema.m_tableTypesWithNames.get(colName);

			Object obj = data.get(colName);
			try {
				byte[] array = objectToByteArray(type, obj);
				if (type == TableSchema.TYPE_STRING || type == TableSchema.TYPE_BYTEARRAY
						|| type == TableSchema.TYPE_OBJECT)
					eba.append(objectToByteArray(TableSchema.TYPE_INT, array.length));
				eba.append(array);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return eba.bufferCopyWithTrim();
	}

	int addIndexes(DataRow data, long offset) {
		for (String colName : m_schema.m_tableTypesWithNames.keySet()) {
			IndexManager indexFile = m_schema.m_columnsToIndexFilePrefix.get(colName);
			if (indexFile != null) {
				int type = m_schema.m_tableTypesWithNames.get(colName);
				Object obj = data.get(colName);
				try {
					byte[] array = objectToByteArray(type, obj);
					long hash = MurmurHash.hash64(array, array.length);
					if (hash == -3996688691163958177l || hash == -8064181893932269848l) {
						System.out.println(hash);
					}
					indexFile.systemAdd(hash, offset);
					//indexFile.add(hash, offset);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return -1;
				}
			}
		}
		return 0;
	}

	public long addDataBytes(byte[] rowBytes) {
		synchronized (m_writeWorker) {
			long pos = -1;
			// synchronized (m_writeWorker) {
			try {
				// m_writeWorker.goTo(m_writeWorker.sizeBytes());
				pos = this.newRowOffset();
				m_writeWorker.append(TableSchema.MAGIC_BEGIN);
				m_writeWorker.append(objectToByteArray(TableSchema.TYPE_INT, rowBytes.length), rowBytes);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return -1;
			}
			// }
			return pos;
		}
	}

	// [118461, 363413, 1130139, 1561564, 2645748, 2808079]
	public boolean deleteIndexes(IndexManager indexManager, ArrayList<Pair<Long, Long>> hashAndOldPos) {
		for (Pair<Long, Long> long1 : hashAndOldPos) {
			indexManager.updateHash(long1.first(), long1.first(), long1.second(), -1L);
		}
		return true;
	}

	public boolean updateIndexes(HashMap<String, Map.Entry<Long, Long>> oldIndexes,
			HashMap<String, Map.Entry<Long, Long>> oldIndexesPositions) {

		for (String colName : oldIndexes.keySet()) {
			Map.Entry<Long, Long> hashPair = oldIndexes.get(colName);
			Map.Entry<Long, Long> posPair = oldIndexesPositions.get(colName);
			IndexManager idf = getIndex(colName);
			if (idf != null) {
				int colde = idf.updateHash(hashPair.getKey(), hashPair.getValue(), posPair.getKey(),
						posPair.getValue());
			}
		}

		return true;
	}

	public RecordData[] findRecords(String whereCols, Object whereValue) {
		// ArrayList<Long> retLong = new ArrayList<>();
		ArrayList<RecordData> retLong = new ArrayList<>();
		IndexManager index = getIndex(whereCols);
		if (index != null) {
			try {
				return index.getIndexesByHash(objectToHash(m_schema.m_tableTypesWithNames.get(whereCols), whereValue));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		} else {
			// synchronized (m_fileWorker) {
			ByteAbstractWorker fileWorker = getReaderWorker();
			long lastPos = 0;
			fileWorker.goTo(0);
			ArrayList<DataRow> ret = new ArrayList<>();
			while (true) {
				DataRow row1 = new DataRow();
				int readed = readData(row1);
				if (readed == -1)
					break;
				Object rO = row1.get(whereCols);
				if (rO.hashCode() == whereValue.hashCode()) {
					retLong.add(new RecordData(null, lastPos));
					// retLong.add(lastPos);
				}
				lastPos = fileWorker.m_position;
				ret.add(row1);
			}
			// m_fileWorker.goTo(lastPos);
			// return ret ;
			// }
		}
		return retLong.toArray(new RecordData[retLong.size()]);
		// return Primitives.arrayListToLongArray(retLong);
	}

	// int lastReadBytes = 0;
	int readData(DataRow ret) {
		// HashMap<String,Object> ret = new HashMap<>();
		// synchronized (m_fileWorker) {
		ByteAbstractWorker fileWorker = getReaderWorker();
		byte[] magicArray = fileWorker.read(4);
		if (magicArray == null)
			return -1;
		try {
			Integer magicNumber = (Integer) byteArrayToObject(TableSchema.TYPE_INT, magicArray);
			if (magicNumber != TableSchema.MAGIC_NUMBER) {
				System.out.println("Error! Magic number not compared! Please repair table.");
				return -1;
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		fileWorker.shift(4);

		byte[] sizeArray = fileWorker.read(4);
		if (sizeArray == null) {
			return -1;
		}
		fileWorker.shift(4);
		try {
			Integer size = (Integer) byteArrayToObject(TableSchema.TYPE_INT, sizeArray);
			boolean deleted = false;
			if (size < 0) {
				/*
				 * int skip = -1*size; m_fileWorker.shift(skip); ret = null; return 0 ;
				 */
				size = -1 * size;
				deleted = true;
			}
			byte[] data = fileWorker.read(size);
			if (data == null) {
				return -1;
			}
			fileWorker.shift(size);
			int position = 0;
			for (String colName : m_schema.m_tableTypesWithNames.keySet()) {
				int type = m_schema.m_tableTypesWithNames.get(colName);
				int dataSize = 4;
				if (type == TableSchema.TYPE_LONG || type == TableSchema.TYPE_DOUBLE)
					dataSize = 8;
				if (type == TableSchema.TYPE_STRING || type == TableSchema.TYPE_BYTEARRAY
						|| type == TableSchema.TYPE_OBJECT) {
					Integer blockSize = (Integer) byteArrayToObject(TableSchema.TYPE_INT, data, position, 4);
					position += 4;
					dataSize = blockSize;
				}
				Object o = byteArrayToObject(type, data, position, dataSize);
				position += dataSize;
				ret.put(colName, o);
				if (deleted) {
					ret.deleted = 1;
				}
			}
			return size + 8;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}

		// }
		// return -1 ;
	}

	public void printStackAtPos(long pos, int sizeLeft, int sizeRight) {
		try {
			ByteAbstractWorker fileWorker = getReaderWorker();
			long cur = fileWorker.getPosition();
			long d = pos - sizeLeft;
			if (d < 0) {
				sizeLeft += d;
			}
			fileWorker.goTo(pos - sizeLeft);
			byte[] buf = fileWorker.read(sizeLeft + sizeRight);
			for (int i = 0; i < buf.length; i++) {
				if (i == sizeLeft)
					System.out.print("[");
				System.out.print(buf[i]);
				if (i == sizeLeft)
					System.out.print("]");
				System.out.print(" ");
			}
			System.out.println("");
			fileWorker.goTo(cur);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ArrayList<DataRow> getData(String name, Object value) {
		ByteAbstractWorker fileWorker = getReaderWorker();
		// synchronized (m_fileWorker) {
		ArrayList<DataRow> ret = new ArrayList<>();
		int type = m_schema.m_tableTypesWithNames.get(name);
		IndexManager indexFile = m_schema.m_columnsToIndexFilePrefix.get(name);
		// indexFile.dump("");

		byte[] array;
		try {
			array = objectToByteArray(type, value);
			long hash = MurmurHash.hash64(array, array.length);
			// byte [] hashArray = ByteBuffer.allocate(8).putLong(hash).array();
			// -3996688691163958177 ? -8064181893932269848
			// long[] indexes = indexFile.getIndexesByHash(hash);
			RecordData[] indexes = indexFile.getIndexesByHash(hash);
			for (RecordData i : indexes) {
				if (i.position == -1)
					continue;
				// byte [] posArray = m_positionFileWorker.readInPos(i*8, 8);
				// long position = ByteBuffer.wrap(posArray).getLong();
				fileWorker.goTo(i.position);
				DataRow obj = new DataRow();
				obj.offset = i.position;
				int r = readData(obj);
				ret.add(obj);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
		// }

	}

	public int count() {
		if (getReaderWorker().sizeBytes() == 0)
			return 0;
		else
			return 1;
	}

	public long newRowOffset() {
		return m_writeWorker.sizeBytes();
	}

	public void databaseDump(String label) {
		ByteAbstractWorker fileWorker = getReaderWorker();
		fileWorker.goTo(0);
		int rows = 0;
		while (true) {
			DataRow row1 = new DataRow();
			int readed = readData(row1);
			if (readed == -1)
				break;
			rows++;
			System.out.println("===== row " + rows + " ====");
			for (String s : row1.keySet()) {
				Object value = row1.get(s);
				System.out.println(s + ":" + value.toString());
			}
		}

	}

	public long addData(DataRow data) {
		synchronized (m_writeQueue) {
			// return systemAddData(data);
			// ToltecDatabase.log("pool size:"+m_addQueue.size());
			WriteQuery wq = new WriteQuery();
			wq.data = data;
			if (m_writeQueue.size() > 100)
				System.out.println("waiting m_addQueue.size > 100");
			while (m_writeQueue.size() > 100) {

				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			m_writeQueue.offer(wq);
			return 0;
		}
	}
	
	public void updateData(DataRow data, String whereCols, Object whereValue) {
		synchronized (m_writeQueue) {
			WriteQuery qw = new WriteQuery();
			qw.data = data;
			qw.whereCols = whereCols;
			qw.whereValue = whereValue;
			qw.operation = WriteQuery.OP_UPDATE;
			if (m_writeQueue.size() > 100)
				System.out.println("waiting m_addQueue.size > 100");
			while (m_writeQueue.size() > 100) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			m_writeQueue.offer(qw);
		}
	}

	public void deleteData(String whereCols, Object whereValue) {
		synchronized (m_writeQueue) {

			WriteQuery qw = new WriteQuery();
			// qw.data = data;
			qw.whereCols = whereCols;
			qw.whereValue = whereValue;
			qw.operation = WriteQuery.OP_DELETE;
			if (m_writeQueue.size() > 100)
				System.out.println("waiting m_addQueue.size > 100");
			while (m_writeQueue.size() > 100) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			m_writeQueue.offer(qw);
		}
	}

	public void systemDeleteData(String whereCols, Object whereValue) {
		synchronized (m_writeWorker) {
			ByteAbstractWorker fileWorker = getReaderWorker();
			byte[] array;
			long deleteHash = 0;
			IndexManager indexFile = m_schema.m_columnsToIndexFilePrefix.get(whereCols);
			try {

				int type1 = m_schema.m_tableTypesWithNames.get(whereCols);
				array = objectToByteArray(type1, whereValue);
				long hash = MurmurHash.hash64(array, array.length);
				deleteHash = hash;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			ArrayList<Pair<Long, Long>> hashAndOldPos = new ArrayList<>();
			// long[] posBytes = findRecords(whereCols, whereValue);
			RecordData[] posBytes = findRecords(whereCols, whereValue);
			for (RecordData l : posBytes) {
				if (l.position == -1)
					continue;
				if (l.position < 0) {
					System.out.println("!");
					findRecords(whereCols, whereValue);
				}
				hashAndOldPos.add(new Pair<Long, Long>(deleteHash, l.position));
				fileWorker.goTo(l.position);
				DataRow readedObject = new DataRow();
				int lenFullBock = readData(readedObject);
				int lenRow = lenFullBock - 4;
				fileWorker.goTo(l.position);
				// m_fileWorker.shift(4); // Info about record length

				HashMap<String, Map.Entry<Long, Long>> oldIndexes = new HashMap<>();
				HashMap<String, Map.Entry<Long, Long>> oldIndexesPositions = new HashMap<>();

				// Create new row HashMap and calculate new index hashes

				// STEP 1: invert len in first 4 bytes like len 100 -> -100 (that mean empty
				// space);
				int nl = lenRow * -1;

				m_writeWorker.goTo(l.position + 4);

				// printStackAtPos(l.position, 10, 10);
				m_writeWorker.writeInt(nl);
				// printStackAtPos(l.position, 10, 10);
			}
			deleteIndexes(indexFile, hashAndOldPos);

			// updateIndexes(oldIndexes, oldIndexesPositions);

		}

	}

	public void systemUpdateData(DataRow data, String whereCols, Object whereValue) {
		synchronized (m_writeWorker) {
			int k = 0;
			ByteAbstractWorker fileWorker = getReaderWorker();
			// ArrayList<HashMap<String, Object>> datagg = getData(whereCols, whereValue);
			IndexManager indexFile = m_schema.m_columnsToIndexFilePrefix.get(whereCols);
			int type1 = m_schema.m_tableTypesWithNames.get(whereCols);
			DataRow obj = new DataRow();
			byte[] array;
			try {
				array = objectToByteArray(type1, whereValue);
				long hash = MurmurHash.hash64(array, array.length);
				// byte [] hashArray = ByteBuffer.allocate(8).putLong(hash).array();
				// -3996688691163958177 ? -8064181893932269848
				// long[] indexes = indexFile.getIndexesByHash(hash);
				RecordData[] indexes = indexFile.getIndexesByHash(hash);
				for (RecordData i : indexes) {
					if (i.position == -1)
						continue;
					fileWorker.goTo(i.position);
					int r = readData(obj);
					// ret.add(obj);
				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			// long[] posBytes = findRecords(whereCols, whereValue);
			RecordData[] posBytes = findRecords(whereCols, whereValue);
			for (RecordData l : posBytes) {
				if (l.position == -1)
					continue;
				fileWorker.goTo(l.position);
				DataRow readedObject = new DataRow();
				int lenFullBock = readData(readedObject);
				int lenRow = lenFullBock - 4;
				fileWorker.goTo(l.position);
				// m_fileWorker.shift(4); // Info about record length
				// boolean needRebuild = false;
				boolean needRebuild = true;
				/*
				 * for (String colName : data.keySet()) { int type =
				 * m_schema.m_tableTypesWithNames.get(colName); int sizeBytes =
				 * TableSchema.typeSizeBites(type); if (sizeBytes == 0) needRebuild = true; }
				 */

				HashMap<String, Map.Entry<Long, Long>> oldIndexes = new HashMap<>();
				HashMap<String, Map.Entry<Long, Long>> oldIndexesPositions = new HashMap<>();

				// Create new row HashMap and calculate new index hashes
				for (String colName : data.keySet()) {
					IndexManager idf = m_schema.getIndex(colName);
					int type = m_schema.m_tableTypesWithNames.get(colName);
					if (idf != null) {
						try {

							byte[] b1 = objectToByteArray(type, data.get(colName));
							byte[] b2 = objectToByteArray(type, readedObject.get(colName));

							Long newIdx = MurmurHash.hash64(b1, b1.length);
							Long oldIdx = MurmurHash.hash64(b2, b2.length);
							// if (newIdx.compareTo(oldIdx)!=0) {
							Pair<Long, Long> pair = new Pair<>(oldIdx, newIdx);
							oldIndexes.put(colName, pair);
							Pair<Long, Long> pairPosition = new Pair<>(l.position, l.position);
							oldIndexesPositions.put(colName, pairPosition);
							// }
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					readedObject.put(colName, data.get(colName));
				}

				if (needRebuild) {
					for (String key : data.keySet()) {
						readedObject.put(key, data.get(key));
					}
					byte[] rowBytes = buildRow(readedObject);
					if (rowBytes.length == lenRow) {
						m_writeWorker.goTo(l.position);
						// new row same length as before. just write.
						m_writeWorker.shift(4 + 4);
						m_writeWorker.write(rowBytes);
					} else {
						// STEP 1: invert len in first 4 bytes like len 100 -> -100 (that mean empty
						// space);
						int nl = lenRow * -1;
						m_writeWorker.goTo(l.position + 4);
						// printStackAtPos(l.position, 10, 10);
						m_writeWorker.writeInt(nl);
						// printStackAtPos(l.position, 10, 10);
						// STEP 2: write new row at end
						long newRowOffset = addDataBytes(rowBytes);
						if (newRowOffset != -1) {
							for (String colName : oldIndexesPositions.keySet()) {
								oldIndexesPositions.get(colName).setValue(newRowOffset);
							}
							if (oldIndexesPositions.containsKey(whereCols) == false && l.hash != null) {
								oldIndexesPositions.put(whereCols, new Pair<>(l.position, newRowOffset));
								oldIndexes.put(whereCols, new Pair<>(l.hash, l.hash));
							}
						} else {
							// I'm don't know what to do. Error there.
						}
					}
					// There we rebuild row and write again to current position or to end.
				} /*
					 * else { // There we update fixed len values for (String colName :
					 * m_schema.m_tableTypesWithNames.keySet()) { int type =
					 * m_schema.m_tableTypesWithNames.get(colName); Object updateObj =
					 * data.get(colName); if (updateObj == null) { // just skip, nothing update
					 * there m_writeWorker.skipBytesByType(type); } else { byte[] updateArray =
					 * null; ; try { updateArray = objectToByteArray(type, updateObj); } catch
					 * (Exception e) { // TODO Auto-generated catch block e.printStackTrace(); } int
					 * sizeBytes = TableSchema.typeSizeBites(type); if (sizeBytes > 0) {
					 * m_writeWorker.write(updateArray); } } } }
					 */
				updateIndexes(oldIndexes, oldIndexesPositions);

			}
		}
	}

	// ConcurrentLinkedQueue<HashMap<String, Object>> m_addQueue = new
	// ConcurrentLinkedQueue<HashMap<String, Object>>();
	// ConcurrentLinkedQueue<HashMap<String, Object>> m_updateQueue = new
	// ConcurrentLinkedQueue<HashMap<String, Object>>();
	ConcurrentLinkedQueue<WriteQuery> m_writeQueue = new ConcurrentLinkedQueue<WriteQuery>();



	long systemAddData(DataRow data) {

		long t1 = System.currentTimeMillis();
		byte[] rowBytes = buildRow(data);
		long t2 = System.currentTimeMillis();
		long ret = addDataBytes(rowBytes);
		long t3 = System.currentTimeMillis();
		addIndexes(data, ret);
		long t4 = System.currentTimeMillis();
		ToltecDatabase.log("build:" + (t2 - t1) + "ms | out:" + (t3 - t2) + "ms | index:" + (t4 - t3) + "ms | total:"
				+ (t4 - t1) + "ms.");
		return ret;

	}

	public ArrayList<DataRow> readAll(int offset, int count ) {
		ByteAbstractWorker fileWorker = getReaderWorker();
		// synchronized (m_fileWorker) {
		long lastPos = fileWorker.m_position;
		fileWorker.goTo(0);
		ArrayList<DataRow> ret = new ArrayList<>();
		int c= 0 ;
		while (true) {
			DataRow row1 = new DataRow();
			row1.offset = fileWorker.m_position;
			// row1.put("offset", fileWorker.m_position);
			int readed = readData(row1);
			if (readed == 0) {
				continue;
			}
			if (readed == -1)
				break;
			c++;
			if (c>offset) {
				ret.add(row1);
			}
			if (count>0&&ret.size()>=count) {
				break ;
			}
		}
		fileWorker.goTo(lastPos);
		return ret;
		
	}
	public ArrayList<DataRow> readAll() {
		return readAll (0,-1);

	}

	public boolean m_QueryEmptry = true;

	public void run() {
		while (true) {

			while (m_writeQueue.isEmpty() == false) {
				m_QueryEmptry = false;
				WriteQuery wq = m_writeQueue.poll();
				// HashMap<String, Object> addObj = m_addQueue.poll();
				if (wq.operation == WriteQuery.OP_ADD) {
					systemAddData(wq.data);
				}
				if (wq.operation == WriteQuery.OP_UPDATE) {
					systemUpdateData(wq.data, wq.whereCols, wq.whereValue);
				}
				if (wq.operation == WriteQuery.OP_DELETE) {
					systemDeleteData(wq.whereCols, wq.whereValue);
				}

			}
			m_QueryEmptry = true;
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
