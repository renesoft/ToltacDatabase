package ToltecDatabase;
import java.util.ArrayList;

public class DataUnit {
	/*
	 * 
	 *  data file :  [ [ Data pair 1 N bites ] [ Data pair 2 N bites ] ]
	 *  index file : [ index of unit 4 (8 bites) ] [ offset 8 bytes ] 
	 * 
	 */
	ArrayList<DataPair> m_dataPairs = new ArrayList<>();
}
