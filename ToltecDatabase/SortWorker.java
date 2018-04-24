package ToltecDatabase;


public class SortWorker {
	
	public static void qSort(ByteAbstractWorker array,  int low, int high) {
	      int i = low;
	      int j = high;
	      byte [] x = array.readInPos(((low+high)/2)*12, 8);
	      //long x = A[(low+high)/2];
	      do {
	    	  
	    	 
	         //while(A[i] < x) ++i;
	    	  byte [] ai = null;
	    	  while ( true ) {
	    		  ai = array.readInPos(i*12, 8);
	    		  int c = matchArrays(ai,x); 
	    		  if (c < 0) ++i;
	    		  else 
	    			  break ;
	    	  }
	         
	         //while(A[j] > x) --j;
	    	  byte [] aj = null;
	    	  while ( true ) {
	    		  aj = array.readInPos(j*12, 8);
	    		  int c = matchArrays(aj,x); 
	    		  if (c > 0) --j;
	    		  else 
	    			  break ;
	    	  }
	    	  
	    	  
	         if(i <= j){
	        	//long temp = A[i];
	        	 byte [] temp = ai;
	        	 //A[i] = A[j];
	        	 array.writeInPos(i*12, aj);//;(block);
	        	 //A[j] = temp;
	        	 array.writeInPos(j*12, temp);//;(block);
	            
	        	 byte [] bi = array.readInPos(i*12+8, 4);
	        	 byte [] bj = array.readInPos(j*12+8, 4);
	            byte[] tempB = bi;
	            
	            //B[i] = B[j];
	            array.writeInPos(i*12+8, bj);
	            //B[j] = tempB;
	            array.writeInPos(j*12+8, tempB);
	            i ++ ; j --;
	         }
	      } while(i <= j);
	      //����������� ������ ������� qSort
	      if(low < j) qSort(array, low, j);
	      if(i < high) qSort(array, i, high);
	  }
	
	public static int matchArrays (byte [] base, byte [] array){
		
		for (int i =0 ; i < base.length; i++){
			int c = base[i]-array[i];
			if (c==0)
				continue;
			else
				return c ;
		}
		return 0 ;
		//return Arrays.equals(readArray,array);
	}
	 public static void qSort(long[] A, int [] B,  int low, int high) {
	      int i = low;
	      int j = high;
	      long x = A[(low+high)/2];
	      do {
	         while(A[i] < x) ++i;
	         while(A[j] > x) --j;
	         if(i <= j){
	        	long temp = A[i];	        	 
	            A[i] = A[j];
	            A[j] = temp;
	            
	            int tempB = B[i];
	            B[i] = B[j];
	            B[j] = tempB;	            
	            i ++ ; j --;
	         }
	      } while(i <= j);
	      //����������� ������ ������� qSort
	      if(low < j) qSort(A, B, low, j);
	      if(i < high) qSort(A, B, i, high);
	  }
	 
	 public static boolean isSorted(long[] a, int limit){
	    //assume is sorted, attempt to prove otherwise
		 int dub = 0 ;
	    for(int i = 0; i < limit; i ++){ //because we are always comparing to the next one and the last one doesn't have a next one we end the loop 1 earlier than usual
	    	if (a[i] == a[i+1]){
	    		dub++;
	    	}
	        if (a[i] > a[i+1]) {
	            return false; //proven not sorted
	        }

	    }
	    System.out.println(dub);
	    return true; //got to the end, must be sorted
	}
}
