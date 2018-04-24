package ToltecDatabase;

public class ByteFileCachedWorker extends ByteAbstractWorker{

	public ByteArrayWorker m_byteArray ;
	public ByteFileWorked m_byteFile ;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ByteFileCachedWorker worker = new ByteFileCachedWorker("C:/tmp/ByteFileCachedWorker1");
		worker.dumpByString("Init");
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
		
		ByteFileCachedWorker worker2 = new ByteFileCachedWorker("C:/tmp/ByteFileCachedWorker2");
		worker.goTo(4);
		worker2.append(worker.read(4));
		worker2.dumpByString("Readed");
		worker2.goTo(0);
		worker.append(worker2.read(4));
		worker.dumpByString("Append back");
	}

	public ByteFileCachedWorker(String name){
		init (name);
	}
	
	@Override
	public void init(String name) {
		// TODO Auto-generated method stub
		m_byteArray = new ByteArrayWorker(name);
		m_byteFile = new ByteFileWorked(name);
		byte [] array = m_byteFile.read((int)m_byteFile.sizeBytes());
		if (array!=null)
			m_byteArray.write(array);
	}

	@Override
	public void append(byte[] block) {
		// TODO Auto-generated method stub
		m_byteArray.append(block);
		m_byteFile.append(block);
	}

	public void append (byte [] block1,byte [] block2) {
		synchronized (m_byteFile) {
			m_byteArray.append(block1);
			m_byteFile.append(block1);
			m_byteArray.append(block2);
			m_byteFile.append(block2);
		}
		
	}
	
	@Override
	public void goTo(long pos) {
		// TODO Auto-generated method stub
		m_byteArray.goTo(pos);
		m_byteFile.goTo(pos);
	}

	@Override
	public void write(byte[] block) {
		m_byteArray.write(block);
		m_byteFile.write(block);
	}

	@Override
	public byte[] read(int size) {
		// TODO Auto-generated method stub
		return m_byteArray.read(size);
	}

	@Override
	public void dumpByString(String label) {
		// TODO Auto-generated method stub
		m_byteFile.goTo(0);
		m_byteArray.goTo(0);
		byte [] fromFile = m_byteFile.read((int)m_byteFile.sizeBytes());
		byte [] fromArray = m_byteArray.read((int)m_byteArray.sizeBytes());
		if (fromFile.length!=fromArray.length){
			System.out.println("Error: array sizes NOT match!");
		}
		for (int i = 0 ; i <fromFile.length; i++){
			if (fromFile[i] != fromArray[i]){
				System.out.println("Error: data in pos ("+i+") NOT match!");
			}
		}
		m_byteArray.dumpByString(label);
		
	}

	@Override
	public long sizeBytes() {
		// TODO Auto-generated method stub
		return m_byteArray.sizeBytes();
	}

	@Override
	public void shift(long pos) {
		m_byteFile.shift(pos);
		m_byteArray.shift(pos);
		
	}
	
	public long getPosition (){
		return m_byteFile.m_position;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		m_byteFile.close();
	}

}
