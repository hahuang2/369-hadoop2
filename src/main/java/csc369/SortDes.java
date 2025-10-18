package csc369;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SortDes {

    public static final Class OUTPUT_KEY_CLASS = LongWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] sa = value.toString().trim().split("\\s+");

            if (sa.length >= 2) {
                StringBuilder country = new StringBuilder();
                for (int i = 0; i < sa.length - 1; i++) {
                    if (i > 0) country.append(" ");
                    country.append(sa[i]);
                }

                long count = Long.parseLong(sa[sa.length - 1]);

                context.write(new LongWritable(-count), new Text(country.toString()));
            }
        }
    }
    public static class ReducerImpl extends Reducer<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void reduce(LongWritable negCount, Iterable<Text> countries, Context context)
                throws IOException, InterruptedException {
            long count = -negCount.get(); // flip back to positive
            for (Text country : countries) {
                context.write(country, new LongWritable(count));
            }
        }
    }
}
