package ToltecDatabase;

import Tools.ExtendableByteArray;

public class ByteArrayWorker extends ByteAbstractWorker{
	
	ExtendableByteArray m_array = null;	
	public ByteArrayWorker (String name){
		init(name);
	}
	
	public void init (String name){
		m_array = new ExtendableByteArray();
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
