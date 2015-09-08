import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        //TODO
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        //Path tmpPath = new Path("/mp2/tmp");
        //fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Find Orphan Pages");
        jobA.setJarByClass(OrphanPages.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        //FileInputFormat.setInputPaths(jobA, tmpPath);
        jobA.setInputFormatClass(TextInputFormat.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setReducerClass(OrphanPageReduce.class);
        //jobA.setNumReduceTasks(1);

        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(jobA, new Path(args[1]));
        //FileOutputFormat.setOutputPath(jobA, tmpPath);
        jobA.setOutputFormatClass(TextOutputFormat.class);

        return jobA.waitForCompletion(true) ? 0 : 1;
        //jobA.waitForCompletion(true);

    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
	public static final Log log = LogFactory.getLog(LinkCountMap.class);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
		String line = value.toString();
            	StringTokenizer tokenizer = new StringTokenizer(line, " :");

		// Page is not necessarily linked to itself
                Integer pageId = Integer.valueOf(tokenizer.nextToken().trim());
		context.write(new IntWritable(pageId), new IntWritable(0));

		// Outlinks only
            	while (tokenizer.hasMoreTokens()) {
                	pageId = Integer.valueOf(tokenizer.nextToken().trim());
//if (nextToken == "school") log.info("Line: " + new Text(line) + "#Token: " + new Text(nextToken));
//if (nextToken == "school") log.debug("Line: " + new Text(line) + "#Token: " + new Text(nextToken));
                    	
			context.write(new IntWritable(pageId), new IntWritable(1));
            	}
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
    	    int nInLinks = 0;
            for (IntWritable val : values) {
                nInLinks += val.get();
            }
	    if (nInLinks == 0)
            	context.write(key, NullWritable.get());
        }
    }
}
