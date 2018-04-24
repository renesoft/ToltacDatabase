package ToltecDatabase;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class TableSchema {
	
	public static int TYPE_NONE= 0;
	public static int TYPE_INT = 1 ;
	public static int TYPE_LONG = 2 ;
	public static int TYPE_FLOAT = 3 ;
	public static int TYPE_DOUBLE = 4 ;
	public static int TYPE_STRING = 5; 
	public static int TYPE_BYTEARRAY = 6 ;
	public static int TYPE_OBJECT = 7 ;
	public static byte [] MAGIC_BEGIN = null;
	public static int  MAGIC_NUMBER = 1234567890;
	static {
		MAGIC_BEGIN = Primitives.IntToByteArray((Integer) MAGIC_NUMBER);
	}
	public static int typeSizeBites (int type){
		if (type == TYPE_INT)
			return Integer.BYTES;
		if (type == TYPE_LONG)
			return Long.BYTES;
		if (type == TYPE_FLOAT)
			return Float.BYTES;
		if (type == TYPE_DOUBLE)
			return Double.BYTES;		
		return 0 ;
	}
		
	
	LinkedHashMap<String, Integer> m_tableTypesWithNames = new LinkedHashMap<>();
	//HashMap<String, Integer> m_tableNamesToHash = new HashMap<>();
	HashMap<String, IndexManager> m_columnsToIndexFilePrefix = new HashMap<>();
	public IndexManager getIndex (String name){
		return m_columnsToIndexFilePrefix.get(name);
	}
}
