package ExternalInterfaces;

import HttpServer.HttpGetDataInterface;
import ToltecDatabase.ByteAbstractWorker;
import ToltecDatabase.DataRow;
import ToltecDatabase.IndexFile;
import ToltecDatabase.IndexManager;
import ToltecDatabase.ToltecDatabase;

public class DatabaseHttpResponce implements HttpGetDataInterface {
	ToltecDatabase m_db = null;
	public DatabaseHttpResponce (ToltecDatabase db){
		m_db = db;
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
		ByteAbstractWorker fileWorker = m_db.getDataFile().getReaderWorker();
		// synchronized (m_fileWorker) {
		long lastPos = fileWorker.m_position;
		fileWorker.goTo(0);

		sb.append("<tr>");
		sb.append("<td>[offset]</td>");
		sb.append("<td>[deleted]</td>");
		for (String colName : m_db.getDataFile().getTableSchema().getTableTypesWithNames().keySet()) {
			sb.append("<td>").append(colName).append("</td>");
		}
		sb.append("</tr>");

		// ArrayList<HashMap<String, Object>> ret = new ArrayList<>();
		while (true) {
			DataRow row1 = new DataRow();
			row1.offset = fileWorker.m_position;
			int readed =m_db.getDataFile().readData(row1);
			if (readed == 0) {
				continue;
			}
			if (readed == -1)
				break;

			sb.append("<tr>");

			sb.append("<td> <a id='offset" + row1.offset + "'>" + row1.offset + "</a></td>");
			sb.append("<td>" + row1.deleted + "</td>");
			for (String colName : m_db.getDataFile().getTableSchema().getTableTypesWithNames().keySet()) {
				sb.append("<td>").append(row1.get(colName)).append("</td>");
			}

		}
		fileWorker.goTo(lastPos);
		sb.append("</table>");
		// }

		for (String colName : m_db.getDataFile().getTableSchema().getTableTypesWithNames().keySet()) {

			IndexManager im = m_db.getDataFile().getIndex(colName);
			if (im == null)
				continue;
			sb.append("<h1>").append(colName).append("</h1>");

			sb.append("<h2>").append(im.getIndexFileMaped().getFileName()).append("</h2>");
			sb.append("<table>");
			ByteAbstractWorker bawMap = im.getIndexFileMaped().getReaderWorker();
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

			for (IndexFile idxf : im.getSortedIndexes()) {
				sb.append("<h2>").append(idxf.getFileName()).append("</h2>");
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
