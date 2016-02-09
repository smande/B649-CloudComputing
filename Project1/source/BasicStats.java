import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BasicStats {
	
	private final static String _delimiter = "|";
	private final static int NUMBER_OF_LINES = 200;

	public static class Map 
            extends Mapper<LongWritable, Text, Text, Text>{
		
		private int _count = 1;
		private double _sum = 0.0;
		private double _squareSum = 0.0;
		private double _min = Double.MAX_VALUE;
		private double _max = Double.MIN_VALUE;
		
		public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			
			double val = new Double(value.toString());
			
			_sum = _sum + val;
			_squareSum = _squareSum + val*val;
			_min = val < _min ? val : _min;
			_max = val > _max ? val : _max;
			
			if (_count == NUMBER_OF_LINES){
				context.write(new Text("COUNT|SUM|MIN|MAX|SQUARE"), 
					new Text(Integer.toString(_count) + _delimiter + 
							Double.toString(_sum) + _delimiter + 
							Double.toString(_min) + _delimiter + 
							Double.toString(_max) + _delimiter + 
							Double.toString(_squareSum)));
			}
			
			_count++;
		}
	}
  
	public static class Reduce
       		extends Reducer<Text,Text,Text,DoubleWritable> {

		private int _localCount = 0;
		private double _sum = 0.0;
		private double _squareSum = 0.0;
		private double _min = Double.MAX_VALUE;
		private double _max = Double.MIN_VALUE;
	  
	  	public void reduce(Text key, Iterable<Text> values, 
	  			Context context) throws IOException, InterruptedException {
      
	  		for (Text val : values) {
	  			String[] splits =  val.toString().split("\\|");
	  			
	  			int count = new Integer(splits[0]);
	  			double sum = new Double(splits[1]);
	  			double min = new Double(splits[2]);
	  			double max = new Double(splits[3]);
	  			double squareSplit = new Double(splits[4]);
	  			
	  			_min = min < _min ? min : _min;
  				_max = max > _max ? max : _max;
  				
  				_localCount = _localCount + count;
  				_sum = _sum + sum;
  				_squareSum = _squareSum + squareSplit;
  				
  				System.out.println("Reduce - COUNT|SUM|MIN|MAX|SQUARE: " +
  									_localCount + _delimiter +
  									_sum + _delimiter +
  									_min + _delimiter +
  									_max + _delimiter +
  									_squareSum);
	  		}
	  		
	  		double avg = _sum/_localCount;
	  		double stddev = Math.sqrt((_squareSum/_localCount) - (avg*avg));
	  		DecimalFormat df = new DecimalFormat("#.##");
	  		df.setRoundingMode(RoundingMode.HALF_DOWN);
	  		avg = new Double(df.format(avg));
	  		stddev = new Double(df.format(stddev));
	  		
	  		
	  		context.write(new Text("MIN: "), new DoubleWritable(_min));
	  		context.write(new Text("MAX: "), new DoubleWritable(_max));
	  		context.write(new Text("AVG: "), new DoubleWritable(avg));
	  		context.write(new Text("STDDEV: "), new DoubleWritable(stddev));
	  	}
	}

  	// Driver program
  	public static void main(String[] args) throws Exception {
    		Configuration conf = new Configuration(); 
    		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
    
	    	if (otherArgs.length != 2) {
	      		System.err.println("Usage: BasicStats <in> <out>");
	      		System.exit(2);
	    	}

		Job job = new Job(conf, "basicstats");
		job.setJarByClass(BasicStats.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
    
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job, new Path(args[0]));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", NUMBER_OF_LINES);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
