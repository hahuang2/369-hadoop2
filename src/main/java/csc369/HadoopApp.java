package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 

	    job.setReducerClass(UserMessages.JoinReducer.class);

	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("HostCountryJoin".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, HostCountryJoin.CountryMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, HostCountryJoin.LogMapper.class ); 

	    job.setReducerClass(HostCountryJoin.JoinReducer.class);

	    job.setOutputKeyClass(HostCountryJoin.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(HostCountryJoin.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	}else if ("HostCountryUrlJoin".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, HostCountryUrlJoin.CountryMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, HostCountryUrlJoin.LogMapper.class ); 

	    job.setReducerClass(HostCountryUrlJoin.JoinReducer.class);

	    job.setOutputKeyClass(HostCountryUrlJoin.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(HostCountryUrlJoin.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	}else if ("UrlCountryList".equalsIgnoreCase(otherArgs[0])) {

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UrlCountryList.CountryMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UrlCountryList.LogMapper.class);

		job.setReducerClass(UrlCountryList.JoinReducer.class);

		job.setOutputKeyClass(UrlCountryList.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(UrlCountryList.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	}	
 	else if ("Sum".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(Sum.ReducerImpl.class);
	    job.setMapperClass(Sum.MapperImpl.class);
	    job.setOutputKeyClass(Sum.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(Sum.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("sortDes".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(SortDes.ReducerImpl.class);
	    job.setMapperClass(SortDes.MapperImpl.class);
	    job.setOutputKeyClass(SortDes.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(SortDes.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}else if ("CountryUrlCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(CountryUrlCount.ReducerImpl.class);
	    job.setMapperClass(CountryUrlCount.MapperImpl.class);
	    job.setOutputKeyClass(CountryUrlCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CountryUrlCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}else if ("SortCountryAZ".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(SortCountryAZ.ReducerImpl.class);
	    job.setMapperClass(SortCountryAZ.MapperImpl.class);
	    job.setOutputKeyClass(SortCountryAZ.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(SortCountryAZ.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}else if ("UrlCountryAgg".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(UrlCountryAgg.ReducerImpl.class);
	    job.setMapperClass(UrlCountryAgg.MapperImpl.class);
	    job.setOutputKeyClass(UrlCountryAgg.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UrlCountryAgg.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}  else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
