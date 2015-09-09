import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(PopularityLeague.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Count & Filter nInLinks");
        jobA.setJarByClass(PopularityLeague.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        //FileInputFormat.setInputPaths(jobA, tmpPath);
        jobA.setInputFormatClass(TextInputFormat.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setReducerClass(LinkCountReduce.class);
        //jobA.setNumReduceTasks(1);

        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        //FileOutputFormat.setOutputPath(jobA, new Path(args[1]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);
        jobA.setOutputFormatClass(TextOutputFormat.class);

        //return jobA.waitForCompletion(true) ? 0 : 1;
        jobA.waitForCompletion(true);

        Job JobB = Job.getInstance(conf, "Sort nInLinks");
        JobB.setJarByClass(PopularityLeague.class);

        //FileInputFormat.setInputPaths(JobB, new Path(args[0]));
        FileInputFormat.setInputPaths(JobB, tmpPath);
        JobB.setInputFormatClass(KeyValueTextInputFormat.class);

        JobB.setMapperClass(LeagueRankMap.class);
        JobB.setMapOutputKeyClass(NullWritable.class);
        JobB.setMapOutputValueClass(IntArrayWritable.class);

        JobB.setReducerClass(LeagueRankReduce.class);
        JobB.setNumReduceTasks(1);

        JobB.setOutputKeyClass(IntWritable.class);
        JobB.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(JobB, new Path(args[1]));
        //FileOutputFormat.setOutputPath(JobB, tmpPath);
        JobB.setOutputFormatClass(TextOutputFormat.class);

        return JobB.waitForCompletion(true) ? 0 : 1;
        //JobB.waitForCompletion(true);
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    // TODO
    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
	public static final Log log = LogFactory.getLog(LinkCountMap.class);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
            	StringTokenizer tokenizer = new StringTokenizer(line, " :");

		// Page is not necessarily linked to itself
                Integer pageId = Integer.valueOf(tokenizer.nextToken().trim());
		//context.write(new IntWritable(pageId), new IntWritable(0));

		// Outlinks only
            	while (tokenizer.hasMoreTokens()) {
                	pageId = Integer.valueOf(tokenizer.nextToken().trim());
//if (nextToken == "school") log.info("Line: " + new Text(line) + "#Token: " + new Text(nextToken));
//if (nextToken == "school") log.debug("Line: " + new Text(line) + "#Token: " + new Text(nextToken));
                    	
			context.write(new IntWritable(pageId), new IntWritable(1));
            	}
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	List<String> leagueList;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String leaguePath = conf.get("league");

            leagueList = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
	    //for (String pageIdString : leagueListString)
	    //	leagueList.add(Integer.valueOf(pageIdString));
        }

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
	    if (!this.leagueList.contains(Integer.toString(key.get())))
		return;

    	    int nInLinks = 0;
            

	    for (IntWritable val : values) {
                nInLinks += val.get();
            }
	    
            context.write(key, new IntWritable(nInLinks));
        }
    }

    public static class LeagueRankMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        private TreeSet<Pair<Integer, Integer>> nInLinksPageIdSet = new TreeSet<Pair<Integer, Integer>>();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

	        Integer pageId = Integer.parseInt(key.toString());
            Integer nInLinks = Integer.parseInt(value.toString());

            nInLinksPageIdSet.add(new Pair<Integer, Integer>(nInLinks, pageId));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Pair<Integer, Integer> item : nInLinksPageIdSet) {
                Integer[] ints = {item.first, item.second};
                IntArrayWritable val = new IntArrayWritable(ints);
                context.write(NullWritable.get(), val);
            }
        }
    }
    
    public static class LeagueRankReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        private TreeSet<Pair<Integer, Integer>> nInLinksPageIdSet = new TreeSet<Pair<Integer, Integer>>();

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {

            for (IntArrayWritable val: values) {
                IntWritable[] pair= (IntWritable[]) val.toArray();

                Integer nInLinks = pair[0].get();
                Integer pageId = pair[1].get();

                nInLinksPageIdSet.add(new Pair<Integer, Integer>(nInLinks, pageId));
            }

	    int rankInt = -1; int prevNInLinks = Integer.MIN_VALUE; int nPrevNInLinks = 1;	
            for (Pair<Integer, Integer> item: nInLinksPageIdSet) {
                IntWritable pageId = new IntWritable(item.second);            
                IntWritable nInLinks = new IntWritable(item.first);
		int nInLinksInt = nInLinks.get();

		if (nInLinksInt > prevNInLinks) {
			rankInt+= nPrevNInLinks; prevNInLinks = nInLinksInt; nPrevNInLinks = 1;
		} else
		if (nInLinksInt == prevNInLinks) {
			nPrevNInLinks++;
		}
	
		IntWritable rankWritable = new IntWritable(rankInt);
                
		//context.write(pageId, nInLinks);
                context.write(pageId, rankWritable);
            }
        }
    }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change

