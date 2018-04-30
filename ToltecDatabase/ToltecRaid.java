package ToltecDatabase;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ExternalInterfaces.RaidHttpResponce;
import HttpServer.HttpGetDataInterface;
import HttpServer.MultiThreadedServer;
import Tools.ExtendableByteArray;

public class ToltecRaid {

	HashMap<Integer, ToltecDatabase> m_dbInstances = new HashMap<>();
	String m_primaryIndex = null;
	MultiThreadedServer m_httpServer = null;
	HttpGetDataInterface m_HttpGetDataInterface = new RaidHttpResponce(this);
	public void addInstance(int code, String folderName, String indexFolder, String databaseName) {
		ToltecDatabase db = new ToltecDatabase();
		db.init(folderName, indexFolder, databaseName);
		m_dbInstances.put(code, db);
	}

	public void monitor(int port) {
		m_httpServer = new MultiThreadedServer(port, m_HttpGetDataInterface);

		new Thread(new Runnable() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
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

	public HashMap<Integer, ToltecDatabase> getInstances (){
		return m_dbInstances;				
	}
	
	public void setPrimaryIndex(String name) {
		m_primaryIndex = name;
	}

	public void addCol(String name, int type, boolean index) {
		for (Integer i : m_dbInstances.keySet()) {
			m_dbInstances.get(i).addCol(name, type, index);// .addColumn (name,type,index,true);
		}
	}

	public DataRow getDataFirst(String field, String value) {
		ArrayList<DataRow> ret = getData(field, value);
		if (ret.size() > 0) {
			return ret.get(0);
		} else {
			return null;
		}
	}

	public ArrayList<DataRow> getData(String field, String value) {
		if (field.compareTo(m_primaryIndex) == 0) {
			int code = value.hashCode();
			if (code < 0)
				code = code * -1;
			code = code % m_dbInstances.size();
			return m_dbInstances.get(code).getData(field, value);
		} else {
			ArrayList<DataRow> ret = new ArrayList<>();
			for (Integer i : m_dbInstances.keySet()) {
				ret.addAll(m_dbInstances.get(i).getData(field, value));
			}
			return ret;
			// return m_dataFile.getData(field, value);
		}
	}

	public boolean commitFinished() {
		for (Integer i : m_dbInstances.keySet()) {
			boolean b = m_dbInstances.get(i).m_dataFile.m_QueryEmptry == true
					&& m_dbInstances.get(i).m_dataFile.m_writeQueue.size() == 0;
			if (b == false)
				return false;
		}
		return true;

	}

	public void updateData(DataRow data, String whereCols, Object whereValue) {
		if (whereCols.compareTo(m_primaryIndex) == 0) {
			int code = whereValue.hashCode() % m_dbInstances.size();
			m_dbInstances.get(code).updateData(data, whereCols, whereValue);
		} else {
			for (Integer i : m_dbInstances.keySet()) {
				m_dbInstances.get(i).updateData(data, whereCols, whereValue);
			}
		}

	}

	public void deleteData(String whereCols, Object whereValue) {
		if (whereCols.compareTo(m_primaryIndex) == 0) {
			int code = whereValue.hashCode() % m_dbInstances.size();
			m_dbInstances.get(code).deleteData(whereCols, whereValue);
		} else {
			for (Integer i : m_dbInstances.keySet()) {
				m_dbInstances.get(i).deleteData(whereCols, whereValue);
			}
		}
	}

	public long addData(DataRow data) {
		if (m_primaryIndex != null) {
			Object o = data.get(m_primaryIndex);
			int code = o.hashCode();
			if (code < 0)
				code = code * -1;
			code = code % m_dbInstances.size();
			return m_dbInstances.get(code).addData(data);
			// return m_dataFile.addData(data);
		} else {
			System.out.println("Data can't be add without Primary index define.");
			return -1;
		}

	}

	public int size() {
		int sum = 0;
		for (Integer i : m_dbInstances.keySet()) {
			for (String key : m_dbInstances.get(i).m_dataFile.m_schema.m_columnsToIndexFilePrefix.keySet()) {
				IndexManager im = m_dbInstances.get(i).m_dataFile.getIndex(key);
				sum += im.countElements();
			}
		}
		return sum;
	}
	
	public void repair (int node) {
		m_dbInstances.get(node).repair();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ToltecRaid raid = new ToltecRaid();
		raid.addInstance(0x0, "D:/tmp_tdb/RaidDatat0x0", "X:/tmp/RaidIndex0x0", "Raid_db");
		raid.addInstance(0x1, "F:/tmp_tdb/RaidDatat0x1", "X:/tmp/RaidIndex0x1", "Raid_db");
		raid.addInstance(0x2, "G:/tmp_tdb/RaidDatat0x2", "X:/tmp/RaidIndex0x2", "Raid_db");
		raid.addInstance(0x3, "H:/tmp_tdb/RaidDatat0x3", "X:/tmp/RaidIndex0x3", "Raid_db");
		raid.addCol("url", TableSchema.TYPE_STRING, true);
		raid.addCol("random_hash", TableSchema.TYPE_INT, false);
		raid.addCol("data", TableSchema.TYPE_BYTEARRAY, false);
		raid.addCol("end", TableSchema.TYPE_INT, false);
		raid.setPrimaryIndex("url");
		int mode = 1;
		if (mode == 1) {
			for (int i = 0; i < 1000; i++) {
				String url = "http://www." + i + ".com";
				ArrayList<DataRow> data = raid.getData("url", url);
				if (data.size() > 0) {
					byte[] buf = (byte[]) data.get(0).get("data");
					System.out.println("" + url + ": " + buf.length + " bytes | instances " + data.size());
				} else {
					System.out.println("" + url + ": | instances " + data.size());
				}

			}
		}
		if (mode == 0) {
			String[] array = new String[4];
			for (int i = 1; i < 5; i++) {
				String fileName = "D:\\tmp_tdb\\" + i + ".html";
				try {
					FileInputStream fis = new FileInputStream(fileName);
					ExtendableByteArray eba = new ExtendableByteArray();
					eba.writeFromInputStream(fis);
					byte[] buf = eba.bufferCopyWithTrim();
					String s = new String(buf);
					array[i - 1] = s;
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

			for (int i = 0; i < 1000; i++) {
				String url = "http://www." + i + ".com";
				int file = i % 4;
				DataRow row = DataRow.create().add("url", url).add("random_hash", 0).add("data", array[file].getBytes())
						.add("end", 0);
				raid.addData(row);
			}
		}

	}


}
