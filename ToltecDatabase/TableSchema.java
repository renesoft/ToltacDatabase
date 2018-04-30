package ToltecDatabase;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class TableSchema {
	
	public final static int TYPE_NONE= 0;
	public final static int TYPE_INT = 1 ;
	public final static int TYPE_LONG = 2 ;
	public final static int TYPE_FLOAT = 3 ;
	public final static int TYPE_DOUBLE = 4 ;
	public final static int TYPE_STRING = 5; 
	public final static int TYPE_BYTEARRAY = 6 ;
	public final static int TYPE_OBJECT = 7 ;
	public final static byte [] MAGIC_BEGIN ;
	public final static int  MAGIC_NUMBER = 1234567890;
	static {
		MAGIC_BEGIN = Primitives.IntToByteArray((Integer) MAGIC_NUMBER);
	}
	public static int typeSizeBites (int type){
		switch (type) {
		case TYPE_INT:
			return Integer.BYTES;
		case TYPE_LONG:	
			return Long.BYTES;
		case TYPE_FLOAT:
			return Float.BYTES;
		case TYPE_DOUBLE:
			return Double.BYTES;	
		default:
			break;
		}			
		return 0 ;
	}		
	
	LinkedHashMap<String, Integer> m_tableTypesWithNames = new LinkedHashMap<>();
	HashMap<String, IndexManager> m_columnsToIndexFilePrefix = new HashMap<>();
	public IndexManager getIndex (String name){
		return m_columnsToIndexFilePrefix.get(name);
	}
	public LinkedHashMap<String, Integer> getTableTypesWithNames (){
		return m_tableTypesWithNames;
	}
}
