package ToltecDatabase;

public class ByteArrayWorker extends ByteAbstractWorker{

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ByteArrayWorker worker = new ByteArrayWorker(null);
		worker.append(new byte[] {0,0,0,0,1,1,1,2,2,2});
		worker.dumpByString("Append");
		worker.goTo(2);
		worker.write(new byte[]{9,9,9});
		worker.dumpByString("Change");
		worker.goTo(8);
		worker.write(new byte[]{8,8,8});
		worker.dumpByString("Change with extended");
		worker.append(new byte[] {3,3,3});
		worker.dumpByString("Append again");
		
		ByteArrayWorker worker2 = new ByteArrayWorker(null);
		worker.goTo(4);
		worker2.append(worker.read(4));
		worker2.dumpByString("Readed");
		worker2.goTo(0);
		worker.append(worker2.read(4));
		worker.dumpByString("Append back");
		
	}
	ExtendableByteArray m_array = null;	
	public ByteArrayWorker (String name){
		init(name);
	}
	
	public void init (String name){
		m_array = new ExtendableByteArray(10);
	}
	
	public void append (byte [] block){
		m_array.append(block);
	}
	public void append (byte [] block1,byte [] block2) {
		synchronized (m_array) {
			m_array.append(block1);
			m_array.append(block2);
		}
		
	}
	
	public void goTo (long pos){
		m_position = pos;
	}
	
	public void write (byte [] block){		
		m_array.write((int)m_position,block, 0, block.length);		
	}
	
	public byte [] read (int size){		
		if (m_position+size <= m_array.size()){
			return m_array.read((int)m_position, size).bufferCopyWithTrim();
		}else{
			return null;
		}
	}
	
	public void dumpByString (String label){
		dumpByString(m_array,label);
	}

	@Override
	public long sizeBytes() {		
		return m_array.size();
	}

	@Override
	public void shift(long pos) {
		m_position+=pos;
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
