package ToltecDatabase;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import Tools.Log;

public class ByteFileWorked extends ByteAbstractWorker{
	
	//public boolean m_cachedInMemory = false;
	public boolean m_isFinal = false;
	//public boolean m_isSorted = true ;
	public File m_file = null;
	public FileOutputStream m_fos = null;
	//public FileInputStream m_fis = null;
	public RandomAccessFile m_accessFile = null;
	public long m_fileCountElements = -1;
	public boolean m_initSuccess = false;
	
	public int readBlockInputStream = 10 ;
	
	public long m_fileSize = 0;
	
	public ByteFileWorked(String name) {		
		if (m_isFinal==false){
			m_isFinal=false;			
		}else{
			m_isFinal=true;					
		}
		init(name);
	}

	@Override
	public void init(String name) {
		m_file = new File(name);
		m_fileSize = m_file.length();
		try {
			if (m_isFinal)
				m_accessFile = new RandomAccessFile(m_file, "r");
			else{
				m_accessFile = new RandomAccessFile(m_file, "rw");
				m_accessFile.seek(m_accessFile.length());
			}
			m_fos = new FileOutputStream(m_file,true);
			m_initSuccess= true ;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			m_initSuccess=false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			m_initSuccess=false;
		}		
	}

	@Override
	public void append(byte[] block) {
		// TODO Auto-generated method stub
		/*try {
			m_fos.write(block);
			m_fos.flush();
			m_fileSize+=block.length;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		try {
			m_accessFile.seek(m_fileSize);
			m_accessFile.write(block);
			m_fileSize+=block.length;
		} catch (IOException e) {		
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	public void append (byte [] block1,byte [] block2) {
		/*synchronized (m_fos) {
			try {
				m_fos.write(block1);
				m_fileSize+=block1.length;
				m_fos.write(block2);
				m_fileSize+=block2.length;
				m_fos.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}*/
		try {
			m_accessFile.seek(m_fileSize);
			m_accessFile.write(block1);
			m_fileSize+=block1.length;
			m_accessFile.write(block2);
			m_fileSize+=block2.length;			
		} catch (IOException e) {		
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void goTo(long pos) {
		// TODO Auto-generated method stub
		m_position = pos;
	}

	@Override
	public void write(byte[] block) {
		// TODO Auto-generated method stub
		try {
			m_accessFile.seek(m_position);
			m_accessFile.write(block);
			long ext = (m_position+block.length)-m_fileSize;
			if (ext>0)
				m_fileSize+=ext;							
			/*long check = m_file.length();
			if (check != m_fileSize){
				Log.error("ByteFileWorked::write(byte[] block) - Error calculate");
			}*/
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public byte[] read(int size) {
		byte[] ret = new byte[size];
		if (size> readBlockInputStream){
			try {
				FileInputStream fis = getInputStreamAtPosition();		
				int rsize = fis.read(ret);
				if (rsize<=0)
					return null;
				fis.close();				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}else{
			try {
				m_accessFile.seek(m_position);
				int rsize = m_accessFile.read(ret);
				if (rsize<=0)
					return null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}				
		return ret;
	}
	
	public FileInputStream getInputStreamAtPosition (){
		FileInputStream fis;
		try {
			fis = new FileInputStream(m_file);
			fis.skip(m_position);
			return fis ;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null ;
		
	}

	@Override
	public long sizeBytes() {
		// TODO Auto-generated method stub
		//return m_fileSize;//m_file.length();
		return m_file.length();
	}

	@Override
	public void shift(long pos) {
		m_position+=pos;
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		try {
			m_fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			m_accessFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
