package ToltecDatabase;

public class DataPair {
	/*
	 * 
	 * [ 4 bytes hash ] [ N bits data - type defined in table schema ]
	 * 
	 * 
	 */
	public int m_hashName;
	public byte [] m_data;
}
