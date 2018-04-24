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

import HttpServer.HttpGetDataInterface;
import HttpServer.MultiThreadedServer;

public class ToltecRaid implements HttpGetDataInterface {

	HashMap<Integer, ToltecDatabase> m_dbInstances = new HashMap<>();
	String m_primaryIndex = null;
	MultiThreadedServer httpServer = null;

	public void addInstance(int code, String folderName, String indexFolder, String databaseName) {
		ToltecDatabase db = new ToltecDatabase();
		db.init(folderName, indexFolder, databaseName);
		m_dbInstances.put(code, db);
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

	@Override
	public String responceData(String url) {
		String path = url.replace("HTTP/1.1", "");
		if (path.compareTo("/favicon.ico") == 0)
			return "";
		StringBuffer sb = new StringBuffer();
		StringBuffer menuBuffer = new StringBuffer();
		menuBuffer.append("<a href='/search'>Search</a> | <a href='/browse'>Browse</a><br>");
		try {
			URL uurl = new URL("http://localhost" + path);
			// sb.append(uurl.getPath()).append("<br>");
			Map<String, List<String>> data = splitQuery(uurl);
			if (uurl.getPath().compareTo("/search") == 0) {
				return menuBuffer.toString()+httpSearchPage(data);
			}			
			if (uurl.getPath().compareTo("/browse") == 0) {
				return menuBuffer.toString()+httpBrowsePage(data);

			}			
			for (String s : data.keySet()) {
				sb.append(s + ":").append(data.get(s)).append("<br>");
			}
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb.toString();

	}
	
	
	
	
	public String httpBrowsePage (Map<String, List<String>> data) {
		int ioffset = 0 ;
		int size = 100 ;
		try {
			String offset = data.get("offset").get(0);
			ioffset = Integer.parseInt(offset);					
		}catch (Exception e) {
			// TODO: handle exception
		}
		int c = 0 ;
		ArrayList<DataRow> dataset1 = m_dbInstances.get(3).m_dataFile.readAll(ioffset,size/m_dbInstances.size());
		HashMap<Integer,ArrayList<DataRow>> list = new HashMap<>();
		for (int i = 0 ; i < m_dbInstances.size() ; i++) {
			ArrayList<DataRow> dataset = m_dbInstances.get(i).m_dataFile.readAll(ioffset,size/m_dbInstances.size());
			list.put(i, dataset);
		}
		ArrayList<DataRow> finalReuslt = new ArrayList<>();
		try {
			for (int o = 0 ; o < size/m_dbInstances.size(); o++) {
				for (int i = 0 ; i < m_dbInstances.size() ; i++) {
					finalReuslt.add(list.get(i).get(o));
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return generateTable(m_dbInstances.get(0).m_dataFile.m_schema.m_tableTypesWithNames.keySet(),finalReuslt);
	}
	
	public String generateTable (Set<String> cols, ArrayList<DataRow>data) {
		StringBuffer sb = new StringBuffer();
		sb.append("<table>");
		sb.append("<tr>");
		
		sb.append("<td>[offset]</td>");
		sb.append("<td>[deleted]</td>");
		for (String colName : cols) {
			sb.append("<td>").append(colName).append("</td>");	
		}		
		sb.append("</tr>");
		
		for (DataRow row1 : data) {
			sb.append("<tr>");
			
			
			sb.append("<td> <a id='offset"+row1.offset+"'>"+row1.offset+"</a></td>");
			sb.append("<td>"+row1.deleted+"</td>");
			for (String colName : cols) {
				Object o = row1.get(colName);
				if (o instanceof String) {
					o = ((String)o).replace("\n", "\n<br>");
				}
				if (o instanceof byte[]) {
					o = "[Byte array:"+ ((byte[])o).length+" bytes]";
				}
				sb.append("<td>").append(o).append("</td>");	
			}	
			sb.append("</tr>");
		}
		
		sb.append("</table>");
		return sb.toString();
	}
	
	public String httpSearchPage(Map<String, List<String>> data) {
		try {
			StringBuffer sb = new StringBuffer();
			if (data == null || data.size() == 0) {
				sb.append("<form action='/search'>");
				sb.append("<select name='field'>");

				ArrayList<DataRow> columns = m_dbInstances.get(0).m_dataFile.schemaTable.readAll();
				for (DataRow dataRow : columns) {
					sb.append("<option value='").append(dataRow.get("name")).append("'>").append(dataRow.get("name"))
							.append("</option>");
				}

				sb.append("</select>");
				sb.append("<input name='value'>");
				sb.append("<input type='submit' value='search'>");
				sb.append("</form>");

			} else {
				try {
					String field = data.get("field").get(0);
					String value = data.get("value").get(0);
					ArrayList<DataRow> result = getData(field, value.trim());
					
					
					
					sb.append("<style>");
					sb.append("table, th, td {\n");
					sb.append("   border: 1px solid black;\n");
					sb.append("   border-collapse: collapse\n"); 
					sb.append("}\n");
					sb.append("</style>");
					sb.append(generateTable(m_dbInstances.get(0).m_dataFile.m_schema.m_tableTypesWithNames.keySet(),result));

				} catch (Exception e) {
					// TODO: handle exception
				}
			}

			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
			// TODO: handle exception
		}
	}

	public static Map<String, List<String>> splitQuery(URL url) throws UnsupportedEncodingException {
		final Map<String, List<String>> query_pairs = new LinkedHashMap<String, List<String>>();
		String query = url.getQuery();
		if (query == null)
			return null;
		final String[] pairs = query.split("&");
		for (String pair : pairs) {
			final int idx = pair.indexOf("=");
			final String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
			if (!query_pairs.containsKey(key)) {
				query_pairs.put(key, new LinkedList<String>());
			}
			final String value = idx > 0 && pair.length() > idx + 1
					? URLDecoder.decode(pair.substring(idx + 1), "UTF-8")
					: null;
			query_pairs.get(key).add(value);
		}
		return query_pairs;
	}

}
