package xdsoft.flume_hdfs.hbase.util;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

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
public class AsyncHbaseTwitterEventSerializer implements AsyncHbaseEventSerializer {
	private byte[] table;
	private byte[] colFam;
	private Event currentEvent;
	private final List<PutRequest> puts = new ArrayList<PutRequest>();
	private final List<AtomicIncrementRequest> incs = new ArrayList<AtomicIncrementRequest>();
	private byte[] currentRowKey;
	private final byte[] eventCountCol = "eventCount".getBytes();
	private String delim;

	@Override
	public void initialize(byte[] table, byte[] cf) {
		this.table = table;
		this.colFam = cf;
	}

	@Override
	public void setEvent(Event event) {
		this.currentEvent = event;
	}

	@Override
	public List<AtomicIncrementRequest> getIncrements() {
		incs.clear();
		// Increment the number of events received
		incs.add(new AtomicIncrementRequest(table, "totalEvents".getBytes(), colFam, eventCountCol));
		return incs;
	}

	@Override
	public void cleanUp() {
		table = null;
		colFam = null;
		currentEvent = null;
		currentRowKey = null;
	}

	@Override
	public void configure(ComponentConfiguration conf) {
	}

	@Override
	public List<PutRequest> getActions() {

		puts.clear();

		// Split the event body and get the values for the columns
//		MyConsoleWriter.write("*TWEET**********************************************************");
		String eventStr = new String(currentEvent.getBody());
//		MyConsoleWriter.write(eventStr);

		eventStr.split(delim);

		byte[] bCol;
		byte[] bFam;
		byte[] bVal;

		// Insert Relevant Parts of Tweet
		try {
			// Status documentation
			// http://twitter4j.org/javadoc/twitter4j/Status.html
			Status tweet = DataObjectFactory.createStatus(eventStr);

			currentRowKey = String.valueOf(tweet.getId()).getBytes();
			// Hbase Column Family Tweet
			bFam = "tweet".getBytes();

			bCol = "id_str".getBytes();
			bVal = String.valueOf(tweet.getId()).getBytes();
			PutRequest req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

			bCol = "text".getBytes();
			bVal = tweet.getText().getBytes();
			req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

			bCol = "created_at".getBytes();
			bVal = String.valueOf(new Timestamp(tweet.getCreatedAt().getTime())).getBytes();
			req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

			bCol = "source".getBytes();
			bVal = tweet.getSource().getBytes();
			req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

			bCol = "retweet_count".getBytes();
			bVal = String.valueOf(tweet.getRetweetCount()).getBytes();
			req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

			if (tweet.getGeoLocation() != null) {
				bCol = "geo_latitude".getBytes();
				bVal = String.valueOf(tweet.getGeoLocation().getLatitude()).getBytes();
				req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
				puts.add(req);

				bCol = "geo_longitude".getBytes();
				bVal = String.valueOf(tweet.getGeoLocation().getLongitude()).getBytes();
				req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
				puts.add(req);

			}

			bCol = "json_str".getBytes();
			bVal = eventStr.getBytes();
			req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

			// Hbase Column Family User - Info about user at time tweet created
			bFam = "user".getBytes();

			bCol = "screen_name".getBytes();
			bVal = tweet.getUser().getScreenName().getBytes();
			req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

			bCol = "location".getBytes();
			bVal = String.valueOf(tweet.getUser().getLocation()).getBytes();
			req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

			bCol = "followers_count".getBytes();
			bVal = String.valueOf(tweet.getUser().getFollowersCount()).getBytes();
			req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

			bCol = "user_name".getBytes();
			bVal = String.valueOf(tweet.getUser().getName()).getBytes();
			req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

			bCol = "user_lang".getBytes();
			bVal = String.valueOf(tweet.getUser().getLang()).getBytes();
			req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
			puts.add(req);

		} catch (TwitterException e) {
		}

		return puts;
	}

	@Override
	public void configure(Context context) {
		delim = context.getString("delimiter", ",");
	}
}
