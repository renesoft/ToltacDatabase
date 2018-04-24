package ToltecDatabase;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import javax.lang.model.element.QualifiedNameable;

public class IndexFileOld {
	public boolean m_cachedInMemory = false;
	public boolean m_isFinal = false;
	public boolean m_isSorted = true ;
	public File m_file = null;
	public RandomAccessFile m_accessFile = null;
	public long m_fileCountElements = -1;
	public boolean m_initSuccess = false;
	public long [] m_hashCache = new long[0];
	public int [] m_indexCache = new int[0];
	public long m_hashSize = 0;
	
	// For not final index
	public long m_maxSize = 10000;
	
	
	
	
	public IndexFileOld (){
		
	}
	public IndexFileOld (String fileName, boolean isFinal){
		if (isFinal==false){
			m_isFinal=false;
			m_isSorted=false;
		}else{
			m_isFinal=true;
			m_isSorted=true;			
		}
		init(fileName);
	}
	
	public void init (String fileName){
		m_file = new File(fileName);
		try {
			if (m_isFinal)
				m_accessFile = new RandomAccessFile(m_file, "r");
			else{
				m_accessFile = new RandomAccessFile(m_file, "rw");
				m_accessFile.seek(m_accessFile.length());
			}
			m_fileCountElements = m_file.length()/12;
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
		if (m_isFinal==false){
			doCache(true);
		}
		
	}
	
	public void add (long hash, int index){
		if (m_isFinal){
			System.out.println("Index is final!");
			return ;
		}
		try {
			if (m_hashSize==m_hashCache.length)
				resizeCache();
			m_accessFile.writeLong(hash);
			m_accessFile.writeInt(index);
			m_hashCache[(int) m_hashSize] = hash;
			m_indexCache[(int) m_hashSize] = index;
			m_hashSize++;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public void resizeCache (){
		long currentSize = m_hashCache.length;
		if (currentSize == 0 )
			currentSize = m_maxSize;
		long [] hash = new long [(int) (currentSize*1.5)+1];
		int [] index = new int [(int) (currentSize*1.5)+1];
		if (m_hashCache.length>0)
			System.arraycopy( m_hashCache, 0, hash, 0, (int) m_hashCache.length );
		if (m_indexCache.length>0)
			System.arraycopy( m_indexCache, 0, index, 0, (int) m_indexCache.length );
		m_hashCache = hash ;
		m_indexCache = index ;
	}
	
	public boolean doFinalize (){
		SortWorker.qSort(m_hashCache, m_indexCache, 0, (int)m_hashSize);
		if (SortWorker.isSorted(m_hashCache,(int)m_hashSize )==false){
			System.out.println("Wrong sorted!");
			return false ;
		}
		try {
			File newfile = new File (m_file.getAbsolutePath()+".tmp");
			FileOutputStream fos = new FileOutputStream(newfile);
			BufferedOutputStream bos = new BufferedOutputStream(fos);
			DataOutputStream dos = new DataOutputStream(bos);
			for (int i = 0 ; i < m_hashSize ; i++){
				dos.writeLong(m_hashCache[i]);
				dos.writeInt(m_indexCache[i]);
			}
			dos.close();
			bos.close();
			fos.close();
			
			if (m_accessFile!=null){
				m_accessFile.close();
			}
			
			m_file.delete();
			newfile.renameTo(m_file);	
			m_isFinal=true;
			m_isSorted=true;
			clearCache();
			init(m_file.getAbsolutePath());
			return true; 
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;		
	}
	
	
	public void clearCache (){
		m_hashCache= new long[0];
		m_indexCache=new int[0];
		m_hashSize = 0;
	}
	public void doCache (boolean reserve){
		if (!m_initSuccess){
			System.out.println("Error in init file.");
			return ;
		}
		boolean removeCache = false;
		m_cachedInMemory=true ;
		
		int count = (int)m_fileCountElements; 
		if (reserve==false){
			m_hashCache = new long[count+1];
			m_indexCache = new int[count+1];
		}else{
			m_hashCache = new long[(int)(count*1.5)+1];
			m_indexCache = new int[(int)(count*1.5)+1];			
		}
		m_hashSize=count ;
		
		try {
			FileInputStream fos = new FileInputStream(m_file);
			BufferedInputStream bis = new BufferedInputStream(fos);
			DataInputStream dis = new DataInputStream(bis);
			for (int i = 0 ; i < m_hashSize; i++){
				try {
					m_hashCache[i] = dis.readLong();
					m_indexCache[i] = dis.readInt();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					removeCache=true;
					
					break ;				
				}				
			}
			
			dis.close();
			bis.close();
			fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();			
			removeCache=true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		if (removeCache){
			m_cachedInMemory=false;
			m_hashCache=null;
			m_indexCache=null;
			m_hashSize=0;
		}
		
		
	}
	
	public int getIndexByHash (long hash){
		if (m_cachedInMemory){
			int position = BinarySearch.binarySearch(m_hashCache, hash,0,(int)m_hashSize-1);//Arrays.binarySearch(m_hashCache,0,(int)m_hashSize-1,hash);
			if (position!=-1)
				return m_indexCache[position];
		}else{
			//   -100 -10 2 15 143 ? 4
			int position;
			try {
				position = BinarySearch.indexOf(m_accessFile, hash);
				return position;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}				
		return -1 ;
	}
	
	public static void main(String[] args) {

	}

}
