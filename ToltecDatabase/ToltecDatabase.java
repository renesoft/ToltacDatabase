package ToltecDatabase;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.GenericArrayType;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import javax.print.attribute.standard.DateTimeAtCompleted;

import HttpServer.HttpGetDataInterface;
import HttpServer.MultiThreadedServer;
import ToltecDatabase.DataRow.DataRowReader;

public class ToltecDatabase implements HttpGetDataInterface {
	DataFileStoreSchema m_dataFile = null;
	String m_folderName = null;
	String m_indexFolderName = null;
	String m_databaseName = null;
	MultiThreadedServer httpServer = null;
	public static int m_beginCode = 1565327231;
	public static int m_endCode = 1327235651;

	public static void log(String text) {
		String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()));
		System.out.println(date + ":[ToltecDatabase] " + text);
	}

	public ToltecDatabase() {

	}

	public int init(String folderName, String indexFolder, String databaseName) {
		this.m_databaseName = databaseName;
		this.m_folderName = folderName;
		this.m_indexFolderName = indexFolder;
		new File(folderName).mkdirs();
		new File(indexFolder).mkdirs();
		String fullPath = new File(new File(folderName), this.m_databaseName).getAbsolutePath();
		m_dataFile = new DataFileStoreSchema(fullPath, indexFolder, false);
		log("Init with path " + fullPath);
		m_dataFile.start();
		return 0;
	}
	
	public void rename (String name) {
		m_dataFile.rename(name);
		for (String key : m_dataFile.m_schema.m_columnsToIndexFilePrefix.keySet()) {
			IndexManager im = m_dataFile.getIndex(key);
			if (im!=null) {
				im.rename(name);
			}
		}
	}
	
	public void monitor(int port) {
		httpServer = new MultiThreadedServer(port, this);

		new Thread(new Runnable() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					httpServer.start();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

	public void addCol(String name, int type, boolean index) {
		m_dataFile.addColumn(name, type, index, true);
	}

	public ArrayList<DataRow> getData(String field, String value) {
		return m_dataFile.getData(field, value);
	}

	public boolean commitFinished() {
		return m_dataFile.m_QueryEmptry == true && m_dataFile.m_writeQueue.size() == 0;
	}

	public void updateData(DataRow data, String whereCols, Object whereValue) {
		m_dataFile.updateData(data, whereCols, whereValue);
	}

	public void deleteData(String whereCols, Object whereValue) {
		m_dataFile.deleteData(whereCols, whereValue);
	}

	public long addData(DataRow data) {
		return m_dataFile.addData(data);
	}

	public void readData(DataRow.DataRowReader reader) {
		readData(reader, false);
	}

	public void readData(DataRow.DataRowReader reader, boolean includeDeleted) {
		ByteAbstractWorker fileWorker = m_dataFile.getReaderWorker();
		// synchronized (m_fileWorker) {

		fileWorker.goTo(0);
		long lastPos = fileWorker.m_position;
		while (true) {
			DataRow row1 = new DataRow();
			row1.offset = fileWorker.m_position;
			int readed = m_dataFile.readData(row1);
			lastPos = fileWorker.m_position;
			if (readed == 0)
				continue;
			if (readed == -1)
				break;
			if (row1.deleted == 1 && includeDeleted == false)
				continue;
			reader.read(row1);
			fileWorker.goTo(lastPos);
		}
		reader.end();

		m_dataFile.removeReaderWorker();
	}

	public void salvageData(DataRow.DataRowReader reader, boolean includeDeleted) {
		ByteAbstractWorker fileWorker = m_dataFile.getReaderWorker();
		// synchronized (m_fileWorker) {

		fileWorker.goTo(0);
		long lastPos = fileWorker.m_position;

		try {
			for (int i = 0; i < fileWorker.sizeBytes() - 3; i++) {
				fileWorker.goTo(i);
				int readedNumber = fileWorker.readInt();
				if (readedNumber == TableSchema.MAGIC_NUMBER) {
					DataRow row1 = new DataRow();
					int readed = m_dataFile.readData(row1);
					if (row1.deleted==1&&includeDeleted==false) 
						continue ;
					if (readed > 0) {
						reader.read(row1);
						i+=(readed-1);						
					}
				}

			}
		} catch (Exception e) {
			// TODO: handle exception
		}

		/*
		 * while (true) { DataRow row1 = new DataRow(); row1.offset =
		 * fileWorker.m_position; int readed = m_dataFile.readData(row1); lastPos =
		 * fileWorker.m_position; if (readed == 0) continue; if (readed == -1) {
		 * while(true) { try { int readedNumber = fileWorker.readInt(); }catch
		 * (Exception e) {
		 * 
		 * } } break; } if (row1.deleted==1&&includeDeleted==false) continue ;
		 * reader.read(row1); fileWorker.goTo(lastPos); }
		 */
		reader.end();

		m_dataFile.removeReaderWorker();
	}

	public int size() {
		for (String key : m_dataFile.m_schema.m_columnsToIndexFilePrefix.keySet()) {
			IndexManager im = m_dataFile.getIndex(key);
			return im.countElements();
		}
		return -1;
	}
	
	
	public void close () {
		for (String colName : m_dataFile.m_schema.m_tableTypesWithNames.keySet()) {
			IndexManager im = m_dataFile.m_schema.m_columnsToIndexFilePrefix.get(colName);
			if (im!=null) {
				im.m_mapedIndex.close();				
				for (IndexFile index: im.m_sortedIndexes) {
					index.close();
				}
			}
			
		}		
		m_dataFile.close();
	}
	
	public void deleteDatabase () {
		for (String colName : m_dataFile.m_schema.m_tableTypesWithNames.keySet()) {
			IndexManager im = m_dataFile.m_schema.m_columnsToIndexFilePrefix.get(colName);
			if (im!=null) {
				im.m_mapedIndex.delete();				
				for (IndexFile index: im.m_sortedIndexes) {
					index.delete();
				}
			}
			
		}		
		m_dataFile.remove();		
	}

	public void repair() {
		ToltecDatabase repairDatabase = new ToltecDatabase();
		repairDatabase.init(m_folderName, m_indexFolderName, "REPAIR_" + m_databaseName);
		for (String colName : m_dataFile.m_schema.m_tableTypesWithNames.keySet()) {
			boolean isIndex = m_dataFile.m_schema.m_columnsToIndexFilePrefix.get(colName) != null;
			int type = m_dataFile.m_schema.m_tableTypesWithNames.get(colName);
			repairDatabase.addCol(colName, type, isIndex);
		}

		this.salvageData(new DataRowReader() {

			@Override
			public void read(DataRow row) {
				repairDatabase.addData(row);

			}

			@Override
			public void end() {
				// TODO Auto-generated method stub

			}
		},false);
		System.out.println("!");
		close();
		deleteDatabase();
		/*for (String colName : m_dataFile.m_schema.m_tableTypesWithNames.keySet()) {
			IndexManager im = m_dataFile.m_schema.m_columnsToIndexFilePrefix.get(colName);
			if (im!=null) {
				im.m_mapedIndex.close();
				im.m_mapedIndex.delete();
				for (IndexFile index: im.m_sortedIndexes) {
					index.close();
					index.delete();
				}
			}
			
		}		
		m_dataFile.close();
		m_dataFile.remove();*/
		repairDatabase.close();
		repairDatabase.rename(m_databaseName);
		init(m_folderName, m_indexFolderName, m_databaseName);
		System.out.println("Repair done.");
	}

	public static void main(String[] args) {
		ToltecDatabase db = new ToltecDatabase();
		db.init("X:/tmp/TestDBIndex", "X:/tmp/TestDBData", "TestDB");
		db.monitor(8809);
		db.addCol("url", TableSchema.TYPE_STRING, true);
		db.addCol("random_hash", TableSchema.TYPE_INT, false);
		db.addCol("data", TableSchema.TYPE_BYTEARRAY, false);
		db.addCol("end", TableSchema.TYPE_INT, false);

		Random rand = new Random(System.currentTimeMillis());

		db.addData(DataRow.create().add("url", "http://aaa").add("random_hash", rand.nextInt(10000))
				.add("data", "Some data".getBytes()).add("end", 12345678));
		db.addData(DataRow.create().add("url", "http://bbb").add("random_hash", rand.nextInt(10000))
				.add("data", "Some data".getBytes()).add("end", 12345678));
		db.addData(DataRow.create().add("url", "http://ccc").add("random_hash", rand.nextInt(10000))
				.add("data", "Some data".getBytes()).add("end", 12345678));
		// db.updateData(DataRow.create().add("url", "a3"), "url", "http://aaa");
		// db.updateData(DataRow.create().add("url", "a4"), "url", "a3");
		// db.deleteData( "url", "r2");
		// db.del
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	@Override
	public String responceData(String url) {
		StringBuffer sb = new StringBuffer();
		sb.append("<html><body>");
		sb.append("<style>");
		sb.append("table, th, td {\n");
		sb.append("   border: 1px solid black;\n");
		sb.append("   border-collapse: collapse\n");
		sb.append("}\n");
		sb.append("</style>");
		sb.append("<table>");
		ByteAbstractWorker fileWorker = m_dataFile.getReaderWorker();
		// synchronized (m_fileWorker) {
		long lastPos = fileWorker.m_position;
		fileWorker.goTo(0);

		sb.append("<tr>");
		sb.append("<td>[offset]</td>");
		sb.append("<td>[deleted]</td>");
		for (String colName : m_dataFile.m_schema.m_tableTypesWithNames.keySet()) {
			sb.append("<td>").append(colName).append("</td>");
		}
		sb.append("</tr>");

		// ArrayList<HashMap<String, Object>> ret = new ArrayList<>();
		while (true) {
			DataRow row1 = new DataRow();
			row1.offset = fileWorker.m_position;
			int readed = m_dataFile.readData(row1);
			if (readed == 0) {
				continue;
			}
			if (readed == -1)
				break;

			sb.append("<tr>");

			sb.append("<td> <a id='offset" + row1.offset + "'>" + row1.offset + "</a></td>");
			sb.append("<td>" + row1.deleted + "</td>");
			for (String colName : m_dataFile.m_schema.m_tableTypesWithNames.keySet()) {
				sb.append("<td>").append(row1.get(colName)).append("</td>");
			}

			/*
			 * sb.append("</tr>");
			 * 
			 * sb.append("<tr>"); sb.append("<td>"); sb.append("[offset]");
			 * sb.append("</td>");
			 * 
			 * sb.append("<td>"); sb.append(row1.offset); sb.append("</td>");
			 * sb.append("</tr>");
			 * 
			 * if (row1.deleted==1) { sb.append("<tr>"); sb.append("<td>");
			 * sb.append("[deleted]"); sb.append("</td>");
			 * 
			 * sb.append("<td>"); sb.append("</td>"); sb.append("</tr>"); }
			 * 
			 * 
			 * for (String key:row1.keySet()) { sb.append("<tr>"); sb.append("<td>");
			 * sb.append(key); sb.append("</td>");
			 * 
			 * sb.append("<td>"); Object data = row1.get(key); sb.append(data);
			 * sb.append("</td>");
			 * 
			 * sb.append("<td>"); IndexManager im = m_dataFile.getIndex(key); if (im!=null)
			 * { int type = m_dataFile.m_schema.m_tableTypesWithNames.get(key); try { long[]
			 * hashes = im.getIndexesByHash(m_dataFile.objectToHash(type, data));
			 * sb.append("["); for (long l : hashes) { sb.append(""+l+","); }
			 * 
			 * sb.append("]"); } catch (Exception e) { // TODO Auto-generated catch block
			 * e.printStackTrace(); } //sb.append(); } sb.append("</td>");
			 * 
			 * sb.append("</tr>"); }
			 * 
			 * sb.append("<tr>"); sb.append("<td>"); sb.append("--------------");
			 * sb.append("</td>"); sb.append("</tr>");
			 */

			// ret.add(row1);
		}
		fileWorker.goTo(lastPos);
		sb.append("</table>");
		// }

		for (String colName : m_dataFile.m_schema.m_tableTypesWithNames.keySet()) {

			IndexManager im = m_dataFile.getIndex(colName);
			if (im == null)
				continue;
			sb.append("<h1>").append(colName).append("</h1>");

			sb.append("<h2>").append(im.m_mapedIndex.m_fileName).append("</h2>");
			sb.append("<table>");
			ByteAbstractWorker bawMap = im.m_mapedIndex.getReaderWorker();
			bawMap.goTo(0);
			for (int i = 0; i < (long) (bawMap.sizeBytes() / 16); i++) {
				long hash = bawMap.readLongShift();
				long pos = bawMap.readLongShift();
				sb.append("<tr>");
				sb.append("<td>");
				sb.append(hash);
				sb.append("</td>");
				sb.append("<td>");
				sb.append("<a href='#offset" + pos + "'>");
				sb.append(pos);
				sb.append("</a>");
				sb.append("</td>");
				sb.append("</tr>");
			}

			sb.append("</table>");

			for (IndexFile idxf : im.m_sortedIndexes) {
				sb.append("<h2>").append(idxf.m_fileName).append("</h2>");

				// im.m_mapedIndex.getReaderWorker()
				// im.m_mapedIndex.getPositionAndIndexesByHash()
				sb.append("<table>");
				ByteAbstractWorker baw = idxf.getReaderWorker();
				baw.goTo(0);
				for (int i = 0; i < (long) (baw.sizeBytes() / 16); i++) {
					long hash = baw.readLongShift();
					long pos = baw.readLongShift();
					sb.append("<tr>");
					sb.append("<td>");
					sb.append(hash);
					sb.append("</td>");
					sb.append("<td>");
					sb.append("<a href='#offset" + pos + "'>");
					sb.append(pos);
					sb.append("</a>");
					sb.append("</td>");
					sb.append("</tr>");
				}

				sb.append("</table>");
			}
		}

		sb.append("</table>");

		sb.append("</html>");
		return sb.toString();
	}

}
