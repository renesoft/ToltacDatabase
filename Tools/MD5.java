package Tools;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5 implements Comparable<MD5>{
	byte [] m_data ;
	public MD5 (){
		
	}
	public MD5 (String string){
		m_data = generateByteHash(string);
	}
	public byte [] data (){
		return m_data ;
	}
	public static final char [] md5bytes = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
	public static byte [] generateByteHash (String url){
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
			md.reset();
		    md.update(url.getBytes());
		    byte messageDigest[] = md.digest();
		    return messageDigest;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null ;		
	}
	public static String generateHash (String url){
	    byte messageDigest[] = generateByteHash(url);
		StringBuffer hexString = new StringBuffer();
		for (int i=0;i<messageDigest.length;i++) {
			String s1 =Integer.toHexString(messageDigest[i]+128);
			hexString.append(s1);
		}
		return hexString.toString();
	}
	@Override
	public int compareTo(MD5 arg0) {
		if (arg0.data()==null)
			return -1;
		if (m_data==null)
			return -1;
		for (int i= 0 ; i < m_data.length; i++){
			if (m_data[i]!=arg0.data()[i])
				return -1;
		}
		return 0;
	}

}
