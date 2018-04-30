package ToltecDatabase;

import java.io.File;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import ExternalInterfaces.ITroubleSolver;
import Tools.ExtendableByteArray;
import Tools.Log;
import Tools.MurmurHash;
import Tools.Pair;
import Tools.ThreadLocalManaged;
import Tools.ThreadLocalManaged.ThreadLocalManagedRemoveIterator;

public class DataFile extends Thread {

	public static class WriteQuery {
		public static final int OP_ADD = 0;
		public static final int OP_UPDATE = 1;
		public static final int OP_DELETE = 2;
		DataRow data;
		String whereCols;
		Object whereValue;
		int operation = 0;
	}

	boolean m_isFinal = false;
	TableSchema m_schema = new TableSchema();
	String m_fileName;
	String m_indexFileName;
	final int m_queueMaxSize = 100;
	ThreadLocalManaged<ByteAbstractWorker> m_readWorkers = new ThreadLocalManaged<>();
	LinkedBlockingQueue<WriteQuery> m_writeQueue = new LinkedBlockingQueue<>(m_queueMaxSize);
	ByteAbstractWorker m_writeWorker = null;
	
	public TableSchema getTableSchema () {
		return m_schema;
	}
	public ByteAbstractWorker getReaderWorker() {
		ByteAbstractWorker worker = m_readWorkers.get();
		if (worker == null) {
			worker = new ByteFileWorked(m_fileName);
			m_readWorkers.set(worker);
		}
		return worker;
	}

	public void removeReaderWorker() {		
		ByteAbstractWorker baw = m_readWorkers.remove();
		if (baw!=null)
			baw.close();		
	}
	
	public void removeAllReaderWorker() {		
		synchronized (m_readWorkers) {
			m_readWorkers.removeAll(new ThreadLocalManagedRemoveIterator<ByteAbstractWorker>() {
				@Override
				public void removed(ByteAbstractWorker t) {
					t.close();				
				}
			});	
		}		
	}

	public void close() {
		if (m_writeWorker != null)
			m_writeWorker.close();
		removeAllReaderWorker();
	}

	public void remove() {
		new File(m_fileName).delete();
	}

	public void rename(String name) {
		File f = new File(m_fileName);
		f.renameTo(new File(f.getParentFile(), name));
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
	}

	long objectToHash(int type, Object obj) throws Exception {
		byte[] buf = objectToByteArray(type, obj);
		long hash = MurmurHash.hash64(buf, buf.length);
		return hash;
	}

	byte[] objectToByteArray(int type, Object obj) throws Exception {
		// ExtendableByteArray eba = new ExtendableByteArray(8);
		switch (type) {
		case TableSchema.TYPE_INT:
			if (obj == null)
				obj = new Integer(0);
			return Primitives.IntToByteArray((Integer) obj);
		case TableSchema.TYPE_LONG:
			if (obj == null)
				obj = new Long(0);
			return Primitives.LongToByteArray((Long) obj);
		case TableSchema.TYPE_FLOAT:
			if (obj == null)
				obj = new Float(0);
			return Primitives.FloatToByteArray((Float) obj);
		case TableSchema.TYPE_DOUBLE:
			if (obj == null)
				obj = new Double(0);
			return Primitives.DoubleToByteArray((Double) obj);
		case TableSchema.TYPE_STRING:
			if (obj == null)
				obj = new String();
			return ((String) obj).getBytes("UTF-8");
		case TableSchema.TYPE_BYTEARRAY:
			if (obj == null)
				obj = new byte[0];
			return (byte[]) obj;
		case TableSchema.TYPE_OBJECT:
			// TODO: Add object serialization
			break;
		default:
			break;
		}
		return null;
	}

	Object byteArrayToObject(int type, byte[] array) throws Exception {
		return byteArrayToObject(type, array, 0, array.length);
	}

	Object byteArrayToObject(int type, byte[] array, int offset, int len) throws Exception {

		switch (type) {
		case TableSchema.TYPE_INT:
			return Primitives.IntFromByteArray(array, offset);
		case TableSchema.TYPE_LONG:
			return Primitives.LongFromByteArray(array, offset);
		case TableSchema.TYPE_FLOAT:
			return Primitives.FloatFromByteArray(array, offset);
		case TableSchema.TYPE_DOUBLE:
			return Primitives.DoubleFromByteArray(array, offset);
		case TableSchema.TYPE_STRING:
			return new String(array, offset, len, "UTF-8");
		case TableSchema.TYPE_BYTEARRAY:
			return Arrays.copyOfRange(array, offset, offset + len);
		case TableSchema.TYPE_OBJECT:
			// TODO: Add object serialization
			break;
		default:
			break;
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
			switch (type) {
			case TableSchema.TYPE_INT:
				map.put(pair[0], Integer.parseInt(pair[1]));
				break;
			case TableSchema.TYPE_LONG:
				map.put(pair[0], Long.parseLong(pair[1]));
				break;
			case TableSchema.TYPE_FLOAT:
				map.put(pair[0], Float.parseFloat(pair[1]));
				break;
			case TableSchema.TYPE_DOUBLE:
				map.put(pair[0], Double.parseDouble(pair[1]));
				break;
			case TableSchema.TYPE_STRING:
				map.put(pair[0], pair[1]);
				break;
			default:
				break;
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
					indexFile.systemAdd(hash, offset);
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
			try {
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
				}
				lastPos = fileWorker.m_position;
				ret.add(row1);
			}
		}
		return retLong.toArray(new RecordData[retLong.size()]);
	}

	public int readData(DataRow ret) {
		ByteAbstractWorker fileWorker = getReaderWorker();
		byte[] magicArray = fileWorker.read(4);
		if (magicArray == null)
			return -1;
		try {
			Integer magicNumber = (Integer) byteArrayToObject(TableSchema.TYPE_INT, magicArray);
			if (magicNumber != TableSchema.MAGIC_NUMBER) {
				Log.error("Error! Magic number not compared! Please repair table. ",this);
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
	}	

	public ArrayList<DataRow> getData(String name, Object value) {
		ByteAbstractWorker fileWorker = getReaderWorker();
		ArrayList<DataRow> ret = new ArrayList<>();
		int type = m_schema.m_tableTypesWithNames.get(name);
		IndexManager indexFile = m_schema.m_columnsToIndexFilePrefix.get(name);

		byte[] array;
		try {
			array = objectToByteArray(type, value);
			long hash = MurmurHash.hash64(array, array.length);
			RecordData[] indexes = indexFile.getIndexesByHash(hash);
			for (RecordData i : indexes) {
				if (i.position == -1)
					continue;
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

	public boolean pushToQueue(WriteQuery qw) {
		boolean nextTry = true;
		while (nextTry == true) {
			try {
				while (m_writeQueue.offer(qw, 10, TimeUnit.MILLISECONDS) == false) {
				}
				return true;
			} catch (InterruptedException e) {
				Log.error("m_writeQueue offer interupted");
				switch (ToltecDatabase.getTroubleSolver().dataNotOfferedInQuere(qw)) {
				case ITroubleSolver.ACT_NOTING:
					nextTry = false;
					break;
				case ITroubleSolver.ACT_RETRY:
					break;
				default:
					nextTry = false;
					break;
				}
			}
		}
		return false;

	}

	public long addData(DataRow data) {
		synchronized (m_writeQueue) {
			WriteQuery wq = new WriteQuery();
			wq.data = data;
			pushToQueue(wq);
		}

		return 0;
	}

	public void updateData(DataRow data, String whereCols, Object whereValue) {
		synchronized (m_writeQueue) {
			WriteQuery qw = new WriteQuery();
			qw.data = data;
			qw.whereCols = whereCols;
			qw.whereValue = whereValue;
			qw.operation = WriteQuery.OP_UPDATE;
			pushToQueue(qw);
		}
	}

	public void deleteData(String whereCols, Object whereValue) {
		synchronized (m_writeQueue) {
			WriteQuery qw = new WriteQuery();
			qw.whereCols = whereCols;
			qw.whereValue = whereValue;
			qw.operation = WriteQuery.OP_DELETE;
			pushToQueue(qw);

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
				hashAndOldPos.add(new Pair<Long, Long>(deleteHash, l.position));
				fileWorker.goTo(l.position);
				DataRow readedObject = new DataRow();
				int lenFullBock = readData(readedObject);
				int lenRow = lenFullBock - 8;
				fileWorker.goTo(l.position);
				HashMap<String, Map.Entry<Long, Long>> oldIndexes = new HashMap<>();
				HashMap<String, Map.Entry<Long, Long>> oldIndexesPositions = new HashMap<>();
				// Create new row HashMap and calculate new index hashes
				// STEP 1: invert len in first 4 bytes like len 100 -> -100 (that mean empty
				// space);
				int nl = lenRow * -1;
				m_writeWorker.goTo(l.position + 4);
				// printStackAtPos(l.position, 10, 10);
				m_writeWorker.writeInt(nl);
				fileWorker.goTo(l.position);
				readData(new DataRow());
				// printStackAtPos(l.position, 10, 10);
			}
			deleteIndexes(indexFile, hashAndOldPos);
		}

	}

	public void systemUpdateData(DataRow data, String whereCols, Object whereValue) {
		synchronized (m_writeWorker) {
			int k = 0;
			ByteAbstractWorker fileWorker = getReaderWorker();
			IndexManager indexFile = m_schema.m_columnsToIndexFilePrefix.get(whereCols);
			int type1 = m_schema.m_tableTypesWithNames.get(whereCols);
			DataRow obj = new DataRow();
			byte[] array;
			try {
				array = objectToByteArray(type1, whereValue);
				long hash = MurmurHash.hash64(array, array.length);
				RecordData[] indexes = indexFile.getIndexesByHash(hash);
				for (RecordData i : indexes) {
					if (i.position == -1)
						continue;
					fileWorker.goTo(i.position);
					int r = readData(obj);
				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			RecordData[] posBytes = findRecords(whereCols, whereValue);
			for (RecordData l : posBytes) {
				if (l.position == -1)
					continue;
				fileWorker.goTo(l.position);
				DataRow readedObject = new DataRow();
				int lenFullBock = readData(readedObject);
				int lenRow = lenFullBock - 8;
				fileWorker.goTo(l.position);
				// m_fileWorker.shift(4); // Info about record length
				// boolean needRebuild = false;
				boolean needRebuild = true;
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
						m_writeWorker.shift(4 + 4);
						m_writeWorker.write(rowBytes);
					} else {
						// STEP 1: invert len in first 4 bytes like len 100 -> -100 (that mean empty
						// space);
						int nl = lenRow * -1;
						m_writeWorker.goTo(l.position + 4);
						m_writeWorker.writeInt(nl);
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
				}
				updateIndexes(oldIndexes, oldIndexesPositions);

			}
		}
	}

	long systemAddData(DataRow data) {
		long t1 = System.currentTimeMillis();
		byte[] rowBytes = buildRow(data);
		long t2 = System.currentTimeMillis();
		long ret = addDataBytes(rowBytes);
		long t3 = System.currentTimeMillis();
		addIndexes(data, ret);
		long t4 = System.currentTimeMillis();
		Log.message("build:" + (t2 - t1) + "ms | out:" + (t3 - t2) + "ms | index:" + (t4 - t3) + "ms | total:"
				+ (t4 - t1) + "ms.");
		return ret;

	}

	public ArrayList<DataRow> readAll(int offset, int count) {
		ByteAbstractWorker fileWorker = getReaderWorker();
		// synchronized (m_fileWorker) {
		long lastPos = fileWorker.m_position;
		fileWorker.goTo(0);
		ArrayList<DataRow> ret = new ArrayList<>();
		int c = 0;
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
			if (c > offset) {
				ret.add(row1);
			}
			if (count > 0 && ret.size() >= count) {
				break;
			}
		}
		fileWorker.goTo(lastPos);
		return ret;

	}

	public ArrayList<DataRow> readAll() {
		return readAll(0, -1);

	}

	public boolean m_QueryEmptry = true;

	public void run() {
		m_QueryEmptry = false;
		while (true) {			
			WriteQuery wq = null;
			if (m_writeQueue.size() == 0) {				
				m_QueryEmptry = true;
				synchronized (m_writeQueue) {
					m_writeQueue.notifyAll();	
				}
				
			}
			try {
				wq = m_writeQueue.poll(1, TimeUnit.SECONDS);
				m_QueryEmptry = false;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (wq == null)
				continue;
			switch (wq.operation) {
			case WriteQuery.OP_ADD:
				systemAddData(wq.data);
				break;
			case WriteQuery.OP_UPDATE:
				systemUpdateData(wq.data, wq.whereCols, wq.whereValue);
				break;
			case WriteQuery.OP_DELETE:
				systemDeleteData(wq.whereCols, wq.whereValue);
				break;
			default:
				break;
			}
		}

	}

}
