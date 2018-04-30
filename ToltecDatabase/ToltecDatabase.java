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
import javax.swing.border.EmptyBorder;

import ExternalInterfaces.DatabaseHttpResponce;
import ExternalInterfaces.EmptyTroubleSolver;
import ExternalInterfaces.ITroubleSolver;
import HttpServer.HttpGetDataInterface;
import HttpServer.MultiThreadedServer;
import ToltecDatabase.DataRow.DataRowReader;
import Tools.Log;

public class ToltecDatabase {
	DataFileStoreSchema m_dataFile = null;
	String m_folderName = null;
	String m_indexFolderName = null;
	String m_databaseName = null;
	MultiThreadedServer m_httpServer = null;
	public static int m_beginCode = 1565327231;
	public static int m_endCode = 1327235651;
	public static ITroubleSolver m_troubleSolver = (ITroubleSolver) new EmptyTroubleSolver();	
	public HttpGetDataInterface m_HttpGetDataInterface = new DatabaseHttpResponce(this);
	public ToltecDatabase() {

	}
	
	
	public static void setTroubleSolver (ITroubleSolver solver) {
		m_troubleSolver = solver;		
	}
	public static ITroubleSolver getTroubleSolver () {
		return m_troubleSolver;				
	}
	
	public int init(String folderName, String indexFolder, String databaseName) {
		this.m_databaseName = databaseName;
		this.m_folderName = folderName;
		this.m_indexFolderName = indexFolder;
		new File(folderName).mkdirs();
		new File(indexFolder).mkdirs();
		String fullPath = new File(new File(folderName), this.m_databaseName).getAbsolutePath();
		m_dataFile = new DataFileStoreSchema(fullPath, indexFolder, false);
		Log.message("Init with path " + fullPath);
		m_dataFile.start();
		return 0;
	}
	
	public DataFileStoreSchema getDataFile () {
		return m_dataFile;
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
		m_httpServer = new MultiThreadedServer(port, m_HttpGetDataInterface);
		new Thread(new Runnable() {
			@Override
			public void run() {				
				try {
					m_httpServer.start();
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
	
	public void awaitCommit() {
		while(commitFinished()==false) {
			try {
				synchronized (m_dataFile.m_writeQueue) {
					m_dataFile.m_writeQueue.wait();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
		fileWorker.goTo(0);	
		try {
			for (int i = 0; i < fileWorker.sizeBytes() - 3; i++) {
				fileWorker.goTo(i);
				int readedNumber = fileWorker.readInt();
				if (readedNumber == TableSchema.MAGIC_NUMBER) {
					DataRow row1 = new DataRow();
					int readed = m_dataFile.readData(row1);
					System.out.println("|i"+i+" + "+readed);
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
		close();
		deleteDatabase();		
		repairDatabase.close();
		repairDatabase.rename(m_databaseName);
		init(m_folderName, m_indexFolderName, m_databaseName);
		Log.message("Repair done.");
	}

}
