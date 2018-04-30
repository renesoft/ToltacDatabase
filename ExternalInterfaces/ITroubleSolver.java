package ExternalInterfaces;

import ToltecDatabase.DataFile.WriteQuery;
import ToltecDatabase.DataRow;
import ToltecDatabase.IndexFile;

public interface ITroubleSolver {
	
	public final static int ACT_NOTING = -1;
	public final static int ACT_RETRY = 1;
	
	
	public int dataNotOfferedInQuere (WriteQuery qw);
	public int indexFileAnalizeFailed (IndexFile index);
}
