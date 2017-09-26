package ztest;

import java.sql.Timestamp;
import java.util.Date;


public class TimeStampTest {

	public static void main(String[] args) {
		
		Timestamp ts = new Timestamp(34415218);
		
		Date date = new Date();
		date = ts;
		System.out.println(date);

	}

}
