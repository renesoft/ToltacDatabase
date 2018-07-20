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
import Tools.Log;

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
	
	public void awaitCommit() {
		for (Integer i : m_dbInstances.keySet()) {
			m_dbInstances.get(i).awaitCommit();
		}
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
			if (code < 0)
				code = code * -1;
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
			Log.error("ToltecRaid::addData(DataRow data):Data can't be add without Primary index define.");
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

	public boolean validate (int node) {
		return m_dbInstances.get(node).validate();
	}	
}
