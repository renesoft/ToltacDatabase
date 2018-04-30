package ToltecDatabase;

public class ByteFileCachedWorker extends ByteAbstractWorker{

	public ByteArrayWorker m_byteArray ;
	public ByteFileWorked m_byteFile ;
		
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
