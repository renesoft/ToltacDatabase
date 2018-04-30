package ExternalInterfaces;

import ToltecDatabase.IndexFile;
import ToltecDatabase.DataFile.WriteQuery;

public class EmptyTroubleSolver implements ITroubleSolver{

	@Override
	public int dataNotOfferedInQuere(WriteQuery qw) {
		return ACT_NOTING;		
	}
	
	public int indexFileAnalizeFailed (IndexFile index) {
		return ACT_NOTING;
	}

}
