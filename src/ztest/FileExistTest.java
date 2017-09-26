package ztest;

import java.io.File;

public class FileExistTest {

	public static void main(String[] args) {
		
		String file = "C:\\logs\\online.CommonGjjInfoRequest";
		
		System.out.println(new File(file).exists());

	}

}
