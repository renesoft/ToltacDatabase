package ToltecDatabase;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Primitives {
	public static byte[] IntToByteArray(int value) {
		return new byte[] { (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value };
	}
	public static int IntFromByteArray(byte[] bytes, int offset) {
		return IntFromBytes(bytes[0+offset], bytes[1+offset], bytes[2+offset], bytes[3+offset]);
	}
	public static int IntFromByteArray(byte[] bytes) {

		return IntFromBytes(bytes[0], bytes[1], bytes[2], bytes[3]);
	}

	public static int IntFromBytes(byte b1, byte b2, byte b3, byte b4) {
		return b1 << 24 | (b2 & 0xFF) << 16 | (b3 & 0xFF) << 8 | (b4 & 0xFF);
	}

	public static byte[] LongToByteArray(long value) {
		// Note that this code needs to stay compatible with GWT, which has
		// known
		// bugs when narrowing byte casts of long values occur.
		byte[] result = new byte[8];
		for (int i = 7; i >= 0; i--) {
			result[i] = (byte) (value & 0xffL);
			value >>= 8;
		}
		return result;
	}
	public static long LongFromByteArray(byte[] bytes, int offset) {
		return LongFromBytes(bytes[0+offset], bytes[1+offset], bytes[2+offset], bytes[3+offset], bytes[4+offset], bytes[5+offset], bytes[6+offset], bytes[7+offset]);
	}
	
	public static long LongFromByteArray(byte[] bytes) {

		return LongFromBytes(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]);
	}

	public static long LongFromBytes(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
		return (b1 & 0xFFL) << 56 | (b2 & 0xFFL) << 48 | (b3 & 0xFFL) << 40 | (b4 & 0xFFL) << 32 | (b5 & 0xFFL) << 24
				| (b6 & 0xFFL) << 16 | (b7 & 0xFFL) << 8 | (b8 & 0xFFL);
	}

	public static byte[] FloatToByteArray(float f) {
		return ByteBuffer.allocate(Float.BYTES).putFloat(f).array();
	}
	public static float FloatFromByteArray(byte[] farray,int offset) {
		return ByteBuffer.wrap(farray,offset,Float.BYTES).getFloat();
	}
	public static float FloatFromByteArray(byte[] farray) {
		return ByteBuffer.wrap(farray).getFloat();
	}

	public static byte[] DoubleToByteArray(double f) {
		return ByteBuffer.allocate(Double.BYTES).putDouble(f).array();
	}
	public static double DoubleFromByteArray(byte[] farray,int offset) {
		return ByteBuffer.wrap(farray,offset,Double.BYTES).getDouble();
	}
	public static double DoubleFromByteArray(byte[] farray) {
		return ByteBuffer.wrap(farray).getDouble();
	}
	
	public static long [] arrayListToLongArray (ArrayList<Long> list){
		long [] ret = new long[list.size()];
		for (int i = 0 ; i < list.size(); i++){
			ret[i] = list.get(i);
		}
		return ret ;
	}

}
