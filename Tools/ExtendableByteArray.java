package Tools;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ExtendableByteArray extends OutputStream implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8645748517093035330L;
	public byte [] array = null;
	public int index = 0 ;
	public transient ByteArrayInputStream bais = null ;
	public ExtendableByteArray (int size){
		array = new byte [size];
	}
	public ExtendableByteArray (){
		array = new byte [2000];
	}
	
	public void append (byte[] buffer){
		append(buffer,0,buffer.length);
	}
	
	public String getString (){
		return new String(array,0,index);
	}
	
	public ExtendableByteArray read (int from, int size){
		ExtendableByteArray ret = new ExtendableByteArray(size);
		ret.write(array, from, size);
		return ret;
	}
	
	public void append (byte[] buffer, int offset, int len){
		int count = len ;
		if (index+count>array.length){
			extend(index+count);
		}
		for (int i = 0; i < count ; i++){
			array[index] = buffer[offset+i];
			index++;
		}
		if (bais!=null)
			try {
				bais.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		bais = new ByteArrayInputStream(array,0,index-1);
	}
	
	public void write (int position, byte[] buffer, int offset, int len){
		for (int i = 0; i < len ; i++){
			if (array.length<=position+i){
				extend();
			}
			array[position+i] = buffer[offset+i];
			if (position+i>=index){
				index++;
			}
			//index++;
		}
		
	}
	
	
	public int size (){
		return index;
	}
	public byte [] buffer (){
		return array ;
	}
	public byte [] bufferCopyWithTrim (){
		if (index == array.length)
			return array;
		byte [] buf = new byte[index];
		for (int i =0 ; i < index; i++){
			buf[i] = array[i];
		}
		return buf ;
	}
	
	public void trim (){
		array = bufferCopyWithTrim();
		index = array.length;
	}
	
	public void extend (){
		extend(0);
	}
	public void writeFromInputStream (InputStream is, int count){
		byte buff [] = new byte[1024];
		int needRead = count;
		try {
			while(true){
				if (needRead<=0 )
					break ;				
				int sForRead = 1024;
				if (sForRead> needRead){
					sForRead = needRead;
				}
				int len = is.read(buff,0,sForRead);				
				needRead -= len;								
				append(buff,0,len);
				if (len==-1){
					break ;
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			if (e instanceof java.net.SocketTimeoutException ){
				
			}else{
				e.printStackTrace();
			}
		}				
	}
	public void writeFromInputStream (InputStream is){
		writeFromInputStream(is,Integer.MAX_VALUE);
	}
	
	public void extend (int moreThat){
		int newSize = array.length*2;
		if (newSize <= moreThat){
			newSize = (int) (moreThat*1.3);
		}
		byte [] copy = new byte[newSize];
		for (int i = 0 ; i < array.length ; i++){
			copy[i] = array[i];
		}
		array = copy ;
	}
	
	public static ExtendableByteArray gzip ( ExtendableByteArray data) {
		return ExtendableByteArray.gzip ( data.buffer(), 0, data.size());
	}
	public static ExtendableByteArray gzip ( byte [] data) {
		return ExtendableByteArray.gzip ( data, 0, data.length);
	}
	public static ExtendableByteArray gzip ( byte [] data, int off, int len) {
		ByteArrayOutputStream arrayis = new ByteArrayOutputStream();
		ExtendableByteArray ret =  new ExtendableByteArray();
		GZIPOutputStream zfos;
		try {
			zfos = new GZIPOutputStream(arrayis);
			zfos.write(data,off,len);
			zfos.finish();
			zfos.close();
			ret.append(arrayis.toByteArray());
			arrayis.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return ret ;
		
	}
	public static ExtendableByteArray ungzip ( ExtendableByteArray  data){
		return ExtendableByteArray.ungzip(data.buffer(),0,data.size());
	}
	public static ExtendableByteArray ungzip ( byte [] data){
		return ExtendableByteArray.ungzip(data,0,data.length);
	}
	public static ExtendableByteArray ungzip ( byte [] data, int off, int len ){
		ByteArrayInputStream bis = new ByteArrayInputStream(data,off,len);
		ExtendableByteArray ret = new ExtendableByteArray();
		try {
			GZIPInputStream gzipis = new GZIPInputStream(bis);
			ret.writeFromInputStream(gzipis);
			gzipis.close();
			bis.close();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret ;
	}

	@Override
	public void write(int b) throws IOException {
		// TODO Auto-generated method stub
		byte [] buf = new byte[1];
		buf[0] = (byte) b ;
		this.append(buf);
	}
	
	public void flush(){
		
	}
	
	public void close(){
		
	}
	
	public void write(byte[] b){
		this.append(b);
	}
	
	public void write(byte[] b, int off, int len){
		this.append(b, off, len);
	}
	
	public InputStream toInputStream (){
		return bais;
	}
	
}
