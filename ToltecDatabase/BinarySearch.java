package ToltecDatabase;


import java.io.IOException;
import java.io.RandomAccessFile;

import Tools.SortWorker;

public class BinarySearch {

	public static int binarySearch(long[] array, long value, int left, int right) {
        if (left > right)
              return -1;
        int middle = (left + right) / 2;
        if (array[middle] == value)
              return middle;
        else if (array[middle] > value)
              return binarySearch(array, value, left, middle - 1);
        else
              return binarySearch(array, value, middle + 1, right);           
  }
	
    public static int indexOf(RandomAccessFile raf, long key) throws IOException {
        long lo = 0;
        long hi = raf.length()/12-1;//a.length - 1;
        while (lo <= hi) {
            // Key is in a[lo..hi] or not present.
            long mid = lo + (hi - lo) / 2;
            if (mid<0){
            	
            }
            raf.seek(mid*12);
            long aMid = raf.readLong();
            
            if  (key < aMid) {
            	hi = mid - 1;
            }	            
            else if (key > aMid) {
            	lo = mid + 1;
            }
            else{
            	return raf.readInt();
            }
        }
        return -1;
    }
    
    public static long simpleSearchInByte(ByteAbstractWorker array, byte[] value, long left, long right) {
    	long pos = left ;
    	array.goTo(pos);
    	while(true){    		
    		byte [] data = array.read(8);
    		if (data == null)
    			return -1;
    		if (SortWorker.matchArrays(data, value) == 0){    
    			
    			return pos;    			
    		}    		
    		array.shift(12);
    		pos+=12;
    	}
    }
    
    public static int binarySearchInByte(ByteAbstractWorker array, byte[] value, int left, int right) {
        if (left > right)
              return -1;
        int middle = (left + right) / 2;
        //Arrays.co
        int compare = array.matchInPos(middle*(8+4), value);
        if (compare == 0){
        	for (middle = middle-1; middle>=0; middle--){

             if(array.matchInPos(middle*(8+4), value) !=0)
               	 break ;
       	 }
        	return (middle+1)*12;
        }
        else if (compare > 0)
              return binarySearchInByte(array, value, left, middle - 1);
        else
              return binarySearchInByte(array, value, middle + 1, right);           
  }
    
    
    
    public static long binarySearchAlt(ByteAbstractWorker array, long key) 
    {
             int low = 0;
             int high = (int) ((array.sizeBytes()/16) -1);
             int shift = 0 ;
             while(high >= low) {
                int middle = (low + high) / 2 + shift;
                array.goTo(middle*16);
                long dataMiddle = array.readLong();
                if (dataMiddle==0){                	
                	shift++;
                	continue ;
                }
                 if(dataMiddle == key) {
                	 for (; middle>=0; middle--){
                		 array.goTo(middle*16);
                         dataMiddle = array.readLong();
                         if(dataMiddle != key)
                        	 break ;
                	 }
                     return (middle+1)*16;
                 }
                 if(dataMiddle < key) {
                     low = middle + 1;
                 }
                 if(dataMiddle > key) {
                     high = middle - 1;
                 }
            }
            return -1;
       }

}
