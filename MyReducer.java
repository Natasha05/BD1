
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.*;
import java.util.Map.*;
import java.io.IOException;

/*
 * Storing the output of mapper in HashMap/List 
 * sorted the list accordingly and then write to
 * context.
 */
public class MyReducer extends Reducer<Text, IntWritable, LongWritable, LongWritable> {

	IntWritable _value = new IntWritable();
	HashMap<Long, Long> HashMapReducer = new HashMap<Long, Long>();
	List<Entry<Long, Long>> list = null;
	Configuration conf;
	int k_Num_of_Pages;
	int page_Counter = 1;

	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException 
	{
		int word_count = 0;
		conf = context.getConfiguration();
		k_Num_of_Pages = Integer.parseInt(conf.get("argument4"));
		for (Iterator<IntWritable> it = values.iterator(); it.hasNext();) 
		{
			word_count += it.next().get();

		}
		HashMapReducer.put(Long.parseLong(key.toString()), (long) word_count);

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
	{
		list = new ArrayList<Entry<Long, Long>>(HashMapReducer.entrySet());
		Collections.sort(list, new Comparator<HashMap.Entry<Long, Long>>() 
		{
			public int compare(HashMap.Entry<Long, Long> obj1, HashMap.Entry<Long, Long> obj2) 
			{

				if (obj1.getValue() < obj2.getValue())
					return 1;
				if (obj1.getValue() > obj2.getValue())
					return -1;
				if (obj1.getKey() > obj2.getKey())
					return 1;
				if (obj1.getKey() < obj2.getKey())
					return -1;
				return 0;
			}
		});
		
			for (HashMap.Entry<Long, Long> entry : list) 
			{
				if (page_Counter <= k_Num_of_Pages) 
				{
					context.write(new LongWritable(entry.getKey()), new LongWritable(entry.getValue()));
				} 
				else 
				{
					break;
				}
				page_Counter++;
			}
		
	}
}
