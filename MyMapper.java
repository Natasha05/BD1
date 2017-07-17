import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/*
 * Filtering data according to 
 * timestamp interval passed as argument in command line
 */

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text _key = new Text();
	private IntWritable _value = new IntWritable();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		/*
		 * Used tokenizer to get the required values for filter out the data
		 * from the HDFS(input) file
		 */
		Configuration conf = context.getConfiguration();
		if (isRevisionLine(value.toString())) {

			String[] word_token = value.toString().split(" ");
			SimpleDateFormat simple_date_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			SimpleDateFormat covert_date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date timestamp = null, interval1 = null, interval2 = null;
			try {
				timestamp = simple_date_format.parse(word_token[4]);
				interval1 = simple_date_format.parse(conf.get("argument2"));
				interval2 = simple_date_format.parse(conf.get("argument3"));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			String formattedTime1 = covert_date_format.format(timestamp);
			String formattedTime2 = covert_date_format.format(interval1);
			String formattedTime3 = covert_date_format.format(interval2);

			if ((Timestamp.valueOf(formattedTime1)).after(Timestamp.valueOf(formattedTime2))
					&& (Timestamp.valueOf(formattedTime1).before(Timestamp.valueOf(formattedTime3)))) 
			{
				_key.set(word_token[1]);        // Set article id as key
				_value.set(1);                 // Pass one(count) as value for particular value
				context.write(_key, _value);
			}
		}

	}

	public boolean isRevisionLine(String line) 
	{
		return line.startsWith("REVISION");

	}
}