package ToltecDatabase;
import java.nio.ByteBuffer;
import java.util.Arrays;

public abstract class ByteAbstractWorker {
	public long m_position = 0 ;
	public abstract void init (String name);	
	public abstract void append (byte [] block);
	public abstract void append (byte [] block1,byte [] block2);
	public abstract void goTo (long pos);
	public abstract void shift (long pos);	
	public abstract void write (byte [] block);	
	public abstract byte [] read (int size);
	
	
	public void skipBytesByType(int type){		
		int skipSize = TableSchema.typeSizeBites(type);
		if (skipSize>0){
			shift(skipSize);
			return;
		}		
		int size = readInt();
		shift(4+size);
	}
	
	
	public long readLong (){
		byte [] ar = read(8);
		return Primitives.LongFromByteArray(ar);
	}
	public long readLongShift (){
		byte [] ar = read(8);
		shift(8);
		return Primitives.LongFromByteArray(ar);
	}
	public  int readInt (){
		byte [] ar = read(4);
		return Primitives.IntFromByteArray(ar);		
	}
	
	public void writeInt (int iblock){
		write (Primitives.IntToByteArray(iblock));
	}
	
	public void writeLong (long block){
		write (Primitives.LongToByteArray(block));
	}
	
	
	public byte [] readInPos (long pos, int size){
		goTo(pos);
		return read(size);
	}
	public void writeInPos (long pos, byte [] block){
		goTo(pos);
		write(block);
		
	}
	public int matchInPos (long pos, byte [] array){
		byte [] readArray = readInPos(pos,array.length);
		for (int i =0 ; i < readArray.length; i++){
			int c = readArray[i]-array[i];
			if (c==0)
				continue;
			else
				return c ;
		}
		return 0 ;
		//return Arrays.equals(readArray,array);
	}
	
	public abstract long sizeBytes ();
	public abstract void dumpByString (String label);	
	public void dumpByString (ExtendableByteArray array, String label){
		int c = 0 ; 
		boolean en = false;
		if (label!=null){
			System.out.println("=========== begin "+label+" ===========");
		}
		for (int i = 0 ; i < array.size(); i++){
			System.out.print(""+array.buffer()[i]+" ");
			c++;
			if (c%10==0){
				System.out.println("|");
				en=true;
			}else{
				en = false;
			}
		}
		if (en == false)
			System.out.println("");
	}
	public long getPosition (){
		return m_position;
	}
	
	public abstract void close ();
}
