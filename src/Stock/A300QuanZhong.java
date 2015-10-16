package Stock;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class A300QuanZhong extends BaseQuery {
	private static List<String> QuanZhongs=new ArrayList<String>();
	
	public static void readFile()
	{
		File GroupFile = new File("QuanZhong");
		if (GroupFile.exists()) {
			BufferedReader in;
			try {
				in = new BufferedReader(new FileReader(GroupFile));
				String line;
				while ((line = in.readLine())!= null) {
//					String items[] =line.substring(line.indexOf("\"")+1, line.lastIndexOf("\"")).split(",");
//					Double lastClose=Double.parseDouble(items[2]);
					QuanZhongs.add(line.substring(line.indexOf("\"")+1, line.lastIndexOf("\"")));
				}
				in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else {
			System.out.println("No config file");
		}
	}
	
	public static void analyse() {
		for (String str : QuanZhongs) {
			String items[]=str.split(",");
			String name=items[0];
			Double lastClose=Double.parseDouble(items[2]);
			Double thisLow=Double.parseDouble(items[5]);
			Double lowLastCloseRate=1-thisLow/lastClose;
			Double thisClose=Double.parseDouble(items[1]);
			Double lowThisCloseRate=1-thisLow/thisClose;
			System.out.println(name+" "+lastClose+" "+thisLow+" "+lowLastCloseRate+" "+lowThisCloseRate);
		}
	}
}
