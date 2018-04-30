package Tools;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Log {
	public static String timeMark () {
		String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()));
		return date ;
	}
	public static void error (String message) {
		System.out.println(timeMark()+" [ERROR]:"+message);
	}
	public static void error (String message, Object source) {
		System.out.println(timeMark()+" [ERROR from "+source.toString()+"]:"+message);
	}
	public static void warning (String message) {
		System.out.println(timeMark()+" [warning]:"+message);
	}
	public static void message (String message) {
		System.out.println(timeMark()+" [message]:"+message);
	}
}
