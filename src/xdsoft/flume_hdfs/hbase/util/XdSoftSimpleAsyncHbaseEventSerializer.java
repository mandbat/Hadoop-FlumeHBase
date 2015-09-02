package xdsoft.flume_hdfs.hbase.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

import com.google.common.base.Charsets;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

/**
 * @author Andrey <br/>
 *         <br/>
 * 
 *         <b>Класс импорта, сериализации данных из Twitter </b>
 * 
 *         <b>Порядок запуска методов: </b><br/>
 *         <br/>
 *         <b>При инициализации один раз: </b><br/>
 *         1. configure <br/>
 *         2. initialize <br/>
 *         <br/>
 * 
 *         <b>Далее в цикле для каждого события: </b><br/>
 *         3. setEvent <br/>
 *         4. getAction <br/>
 *         5. getIncrements <br/>
 */
public class XdSoftSimpleAsyncHbaseEventSerializer implements AsyncHbaseEventSerializer {

	private byte[] table;
	private byte[] cf;
	private byte[] payloadColumn;
	private byte[] incrementColumn;

	private byte[] incrementRow;
	private Event currentEvent;
	private byte[] currentRowKey;
	
	// custom
	private byte[][] columnNames;
	private String delim;

	@Override
	public void configure(ComponentConfiguration conf) {
	}

	@Override
	public void initialize(byte[] table, byte[] cf) {
		this.table = table;
		this.cf = cf;
	}

	@Override
	public List<PutRequest> getActions() {

		MyConsoleWriter.write("getActions");

		String eventStr = new String(currentEvent.getBody());
		// String[] cols = eventStr.split(",");

		try {
			Status tweet = DataObjectFactory.createStatus(eventStr);
			currentRowKey = String.valueOf(tweet.getId()).getBytes();
		} catch (TwitterException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		List<PutRequest> actions = new ArrayList<PutRequest>();
		if (payloadColumn != null) {
			byte[] rowKey;
			try {
				rowKey = currentRowKey;
				PutRequest putRequest = new PutRequest(table, rowKey, cf, payloadColumn, eventStr.getBytes());
				actions.add(putRequest);
			} catch (Exception e) {
				throw new FlumeException("Could not get row key!!", e);
			}
		}
		return actions;
	}

	public List<AtomicIncrementRequest> getIncrements() {

		MyConsoleWriter.write("getIncrements");

		List<AtomicIncrementRequest> actions = new ArrayList<AtomicIncrementRequest>();
		if (incrementColumn != null) {
			AtomicIncrementRequest inc = new AtomicIncrementRequest(table, incrementRow, cf, incrementColumn);
			actions.add(inc);
		}
		return actions;
	}

	@Override
	public void cleanUp() {

		MyConsoleWriter.write("cleanUp");

	}

	@Override
	public void configure(Context context) {

		MyConsoleWriter.write("Загрузка конфигурации колонок:");
		
		// Get the column names from the configuration
		String col = new String(context.getString("columns"));
		MyConsoleWriter.write("cols: " + col);
		
		String[] names = col.split(",");
		MyConsoleWriter.write("names: " + Arrays.toString(names));
		
		columnNames = new byte[names.length][];
		
		int i = 0;
		for (String name : names) {
			columnNames[i++] = name.getBytes();
		}
		MyConsoleWriter.write("columnNames: " + Arrays.deepToString(columnNames));
		
		delim = new String(context.getString("delimiter"));
		MyConsoleWriter.write("delim: " + delim);

		// String pCol = context.getString("payloadColumn", "pCol");
		// String iCol = context.getString("incrementColumn", "iCol");

		//payloadColumn = "twitter_text".getBytes(Charsets.UTF_8);
		
		
		incrementColumn = "id".getBytes(Charsets.UTF_8);
		
		//incrementRow = context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
		incrementRow = "incRow".getBytes(Charsets.UTF_8);

	}

	@Override
	public void setEvent(Event event) {

		MyConsoleWriter.write("Event получен: " + event.getBody().toString(), "//111//event.txt");
		this.currentEvent = event;

	}

}
