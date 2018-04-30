package ExternalInterfaces;

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
import ToltecDatabase.DataRow;
import ToltecDatabase.ToltecRaid;

public class RaidHttpResponce implements HttpGetDataInterface {
	ToltecRaid m_raid ;
	public RaidHttpResponce (ToltecRaid raid){
		m_raid = raid;
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
		ArrayList<DataRow> dataset1 = m_raid.getInstances().get(3).getDataFile().readAll(ioffset,size/m_raid.getInstances().size());
		HashMap<Integer,ArrayList<DataRow>> list = new HashMap<>();
		for (int i = 0 ; i < m_raid.getInstances().size() ; i++) {
			ArrayList<DataRow> dataset = m_raid.getInstances().get(i).getDataFile().readAll(ioffset,size/m_raid.getInstances().size());
			list.put(i, dataset);
		}
		ArrayList<DataRow> finalReuslt = new ArrayList<>();
		try {
			for (int o = 0 ; o < size/m_raid.getInstances().size(); o++) {
				for (int i = 0 ; i < m_raid.getInstances().size() ; i++) {
					finalReuslt.add(list.get(i).get(o));
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return generateTable(m_raid.getInstances().get(0).getDataFile().getTableSchema().getTableTypesWithNames().keySet(),finalReuslt);
	}
	
	public String generateTable (Set<String> cols, ArrayList<DataRow>data) {
		StringBuffer sb = new StringBuffer();
		sb.append("<style>");
		sb.append("table, th, td {\n");
		sb.append("   border: 1px solid black;\n");
		sb.append("   border-collapse: collapse\n"); 
		sb.append("}\n");
		sb.append("</style>");
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

				ArrayList<DataRow> columns = m_raid.getInstances().get(0).getDataFile().getSchemaDataFile().readAll();
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
					ArrayList<DataRow> result = m_raid.getData(field, value.trim());
					
					
					
					sb.append("<style>");
					sb.append("table, th, td {\n");
					sb.append("   border: 1px solid black;\n");
					sb.append("   border-collapse: collapse\n"); 
					sb.append("}\n");
					sb.append("</style>");
					sb.append(generateTable(m_raid.getInstances().get(0).getDataFile().getTableSchema().getTableTypesWithNames().keySet(),result));

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
