package ToltecDatabase;

public class RecordData {
	public RecordData () {
		
	}
	public RecordData (long hash,long position) {
		this.hash= hash;
		this.position = position;
	}
	public RecordData (Long hash,long position) {
		this.hash= hash;
		this.position = position;
	}
	long position;
	Long hash = null ;
}
