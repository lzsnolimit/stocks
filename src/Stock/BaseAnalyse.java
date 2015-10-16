package Stock;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;

public class BaseAnalyse {

	/**
	 * Write content in a file
	 * 
	 * @param fileName
	 * @param content
	 * @param append
	 */
	
	public static void writeFile(String fileName, String content, boolean append) {
		File outFile = new File(fileName);
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(outFile,
					append));
			out.write(content + "\r\n");
			out.flush(); // 把缓存区内容压入文件
			out.close(); // 最后记得关闭文件
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
