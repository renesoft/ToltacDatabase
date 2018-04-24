package ToltecDatabase;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class DataRow extends HashMap <String,Object>{	
	public static interface DataRowReader {
		public void read(DataRow row) ;
		public void end() ;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = -4191547905198980127L;
	public long offset = 0 ;
	public int deleted = 0 ;
	
	
	public static DataRow create () {
		DataRow newRow = new DataRow();
		return newRow;
	}
	
	public DataRow add(String key,Object value) {
		put(key, value);
		return this;
	}
	
}
