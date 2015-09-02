package xdsoft.flume_hdfs.hbase.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class MyConsoleWriter {

	/**
	 * Запись текста в файл. Имя файла передается в параметрах вызоа метода.
	 * @param text - текст который необходимо записать в файл
	 * @param fileName - полный путь к файлу в файловой системе
	 */
	public static void write(String text, String fileName) {
		try {
			FileWriter fwr = new FileWriter(fileName, true);
			BufferedWriter out = new BufferedWriter(fwr);
			long curTime = System.currentTimeMillis();
			String curStringDate = new SimpleDateFormat("dd.MM.yyyy H:mm:ss:SSS").format(curTime);
			out.write(curStringDate + ": " + text + "\n");
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Запись текста в файл "D:\\debug.txt"
	 * @param text текст который необходимо записать в файл
	 */
	public static void write(String text) {
		try {
			FileWriter fwr = new FileWriter("D:\\debug.txt", true);
			BufferedWriter out = new BufferedWriter(fwr);
			long curTime = System.currentTimeMillis();
			String curStringDate = new SimpleDateFormat("dd.MM.yyyy H:mm:ss:SSS").format(curTime);
			out.write(curStringDate + ": " + text + "\n");
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}