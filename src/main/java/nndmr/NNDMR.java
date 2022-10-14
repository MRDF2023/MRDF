package nndmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import writables.*;
import utils.*;

import java.io.IOException;
import java.util.*;

public class NNDMR extends Configured implements Tool {

    long mor, mob, momb;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new NNDMR(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        long t;

        Configuration conf = getConf();

        String inputPath = conf.get("input");
        String outputPath = conf.get("output", "result");

        int numVector = conf.getInt("numVectors", 0);
        long KN = numVector * conf.getInt("k", 30);
        int threshold = (int)(KN * conf.getFloat("et", 0.001f));

        String randomGraphTmpPath = outputPath + ".rg1";
        String randomGraphPath = outputPath + ".r0";

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(new Path(randomGraphTmpPath), true);
        fs.delete(new Path(randomGraphPath), true);

        t = System.currentTimeMillis();
        initGraphStep1(inputPath, randomGraphTmpPath);
        System.out.println("r0s1: " + (System.currentTimeMillis() - t) + "\t" + mor + "\t" + mob + "\t" + momb);

        t = System.currentTimeMillis();
        initGraphStep2(randomGraphTmpPath, randomGraphPath);
        System.out.println("r0s2: " + (System.currentTimeMillis() - t) + "\t" + mor + "\t" + mob + "\t" + momb);

        fs.delete(new Path(randomGraphTmpPath), true);

        int round=0;
        long numChanges = Long.MAX_VALUE;
        while(numChanges > threshold)
        {
            String roundInputPath = outputPath + ".r" + round;
            String roundStep1Path = outputPath + ".r" + round + ".s1";
            String roundStep2Path = outputPath + ".r" + round + ".s2";
            String roundStep3Path = outputPath + ".r" + round + ".s3";
            String roundOutputPath = outputPath + ".r" + (++round);

            fs.delete(new Path(roundStep1Path), true);
            fs.delete(new Path(roundStep2Path), true);
            fs.delete(new Path(roundStep3Path), true);
            fs.delete(new Path(roundOutputPath), true);

            t = System.currentTimeMillis();
            runStep1(roundInputPath, roundStep1Path, round);
            System.out.printf("step1\t%2d\t%15d\t%15d\t%15d\t%15d\n", round, (System.currentTimeMillis() - t), mor, mob, momb);

            t = System.currentTimeMillis();
            runStep2(roundStep1Path, roundStep2Path, round);
            System.out.printf("step2\t%2d\t%15d\t%15d\t%15d\t%15d\n", round, (System.currentTimeMillis() - t), mor, mob, momb);

            t = System.currentTimeMillis();
            runStep3(roundStep2Path, roundStep3Path, round);
            System.out.printf("step3\t%2d\t%15d\t%15d\t%15d\t%15d\n", round, (System.currentTimeMillis() - t), mor, mob, momb);

            t = System.currentTimeMillis();
            numChanges = runStep4(roundStep3Path, roundOutputPath, round);
            System.out.printf("step4\t%2d\t%15d\t%15d\t%15d\t%15d\t%d\n", round, (System.currentTimeMillis() - t), mor, mob, momb, numChanges);

            fs.delete(new Path(roundStep1Path), true);
            fs.delete(new Path(roundStep2Path), true);
            fs.delete(new Path(roundStep3Path), true);
            fs.delete(new Path(roundInputPath), true);
        }

        System.out.print("\nConverting Result to TextFile ... ");
        String final_input = outputPath + ".r" + round;
        String final_output = outputPath + ".ep.F";
        fs.delete(new Path(final_output), true);

        writeResult(final_input, final_output);
        fs.delete(new Path(final_input), true);
        System.out.println("Complete");

        return 0;
    }

    public void initGraphStep1(String input, String output) throws Exception
    {
        Job job = Job.getInstance(getConf(), "Init_Graph_Step1");
        job.setJarByClass(NNDMR.class);

        job.setMapperClass(GraphStep1Mapper.class);
        job.setReducerClass(GraphStep1Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeOrNodeVectorWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NodeVectorWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
        momb = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES).getValue();
    }

    public static class GraphStep1Mapper extends Mapper<Object, Text, IntWritable, NodeOrNodeVectorWritable>
    {
        public int dimVector, k, numVectors;
        public float[] u_vec;
        Random rand = new Random();

        IntWritable uw = new IntWritable();
        NodeOrNodeVectorWritable nvw = new NodeOrNodeVectorWritable();

        @Override
        protected void setup(Mapper<Object, Text, IntWritable, NodeOrNodeVectorWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector", 0);
            k = conf.getInt("k", 30);
            numVectors = conf.getInt("numVectors", 0);
            NodeOrNodeVectorWritable.dimVector = dimVector;

            u_vec = new float[dimVector];
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, NodeOrNodeVectorWritable>.Context context) throws IOException, InterruptedException {

            StringTokenizer st = new StringTokenizer(value.toString());

            int u = Integer.parseInt(st.nextToken());
            for(int i = 0 ; i < dimVector ; i++)
            {
                u_vec[i] = Float.parseFloat(st.nextToken());
            }
            uw.set(u);
            nvw.set(u, u_vec);
            context.write(uw, nvw);

            nvw.set(u);
            HashSet<Integer> included = new HashSet<Integer>();
            included.add(u);
            for(int i = 0, v = u ; i < k ; i++)
            {
                while(included.contains(v))
                {
                    v = rand.nextInt(numVectors);
                }
                uw.set(v);
                included.add(v);
                context.write(uw, nvw);
            }
        }
    }

    public static class GraphStep1Reducer extends Reducer<IntWritable, NodeOrNodeVectorWritable, IntWritable, NodeVectorWritable>
    {
        public int dimVector, k;

        NodeVectorWritable nvw = new NodeVectorWritable();
        IntWritable vw = new IntWritable();
        public float[] u_vec;

        @Override
        protected void setup(Reducer<IntWritable, NodeOrNodeVectorWritable, IntWritable, NodeVectorWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector",0);
            k = conf.getInt("k", 30);
            NodeVectorWritable.dimVector = dimVector;
            NodeOrNodeVectorWritable.dimVector = dimVector;

            u_vec = new float[dimVector];
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeOrNodeVectorWritable> values, Reducer<IntWritable, NodeOrNodeVectorWritable, IntWritable, NodeVectorWritable>.Context context) throws IOException, InterruptedException {

            ArrayList<Integer> nodes = new ArrayList<Integer>();

            for(NodeOrNodeVectorWritable nonvw : values)
            {
                if(nonvw.hasVector)
                {
                    System.arraycopy(nonvw.vector, 0, u_vec, 0, dimVector);
                }
                else
                {
                    nodes.add(nonvw.id);
                }
            }

            nvw.set(key.get(), u_vec, true, true, false);
            context.write(key, nvw);

            for(int n : nodes)
            {
                vw.set(n);
                context.write(vw, nvw);
            }
        }
    }

    public void initGraphStep2(String input, String output) throws Exception
    {
        Job job = Job.getInstance(getConf(), "Init_Graph_Step2");
        job.setJarByClass(NNDMR.class);

        job.setMapperClass(GraphStep2Mapper.class);
        job.setReducerClass(GraphStep2Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeVectorWritable.class);

        job.setOutputKeyClass(NodeVectorWritable.class);
        job.setOutputValueClass(KNNSimpleItemWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
        momb = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES).getValue();
    }

    public static class GraphStep2Mapper extends Mapper<IntWritable, NodeVectorWritable, IntWritable, NodeVectorWritable>
    {
        @Override
        protected void setup(Mapper<IntWritable, NodeVectorWritable, IntWritable, NodeVectorWritable>.Context context) {
            NodeVectorWritable.dimVector = context.getConfiguration().getInt("dimVector",0);
        }

        @Override
        protected void map(IntWritable key, NodeVectorWritable value, Mapper<IntWritable, NodeVectorWritable, IntWritable, NodeVectorWritable>.Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class GraphStep2Reducer extends Reducer<IntWritable, NodeVectorWritable, NodeVectorWritable, KNNSimpleItemWritable>
    {
        public int dimVector, k;
        float[][] vectors;

        NodeVectorWritable uw = new NodeVectorWritable();
        KNNSimpleItemWritable knnsw = new KNNSimpleItemWritable();

        @Override
        protected void setup(Reducer<IntWritable, NodeVectorWritable, NodeVectorWritable, KNNSimpleItemWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector",0);
            k = conf.getInt("k", 30);
            NodeVectorWritable.dimVector = dimVector;
            KNNSimpleItemWritable.numNeighbors = k;
            uw.vector = new float[dimVector];
            knnsw.setup();
            vectors = new float[k][];
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeVectorWritable> values, Reducer<IntWritable, NodeVectorWritable, NodeVectorWritable, KNNSimpleItemWritable>.Context context) throws IOException, InterruptedException {

            int i = 0;

            for(NodeVectorWritable nvw : values)
            {
                if(nvw.id == key.get())
                {
                    uw.id = key.get();
                    System.arraycopy(nvw.vector, 0, uw.vector, 0, dimVector);
                }
                else
                {
                    knnsw.neighbors[i] = nvw.id;
                    vectors[i] = nvw.vector;
                    knnsw.flag_new[i] = true;
                    i++;
                }
            }

            assert i == k: "i("+i+") should be the same as k("+k+")";

            for(i = 0 ; i < k ; i++)
            {
                knnsw.distances[i] = Distances.l2(uw.vector, vectors[i]);
            }

            context.write(uw, knnsw);
        }
    }

    public void runStep1(String inputPath, String outputPath, int round) throws Exception
    {
        Job job = Job.getInstance(getConf(), "4R : Round(" + round + ") : Step1");
        job.setJarByClass(NNDMR.class);

        job.setMapperClass(Step1Mapper.class);
        job.setReducerClass(Step1Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeVectorOrSimpleWritable.class);

        job.setOutputKeyClass(NodeVectorWritable.class);
        job.setOutputValueClass(NNSimpleWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
        momb = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES).getValue();
    }

    public static class Step1Mapper extends Mapper<NodeVectorWritable, KNNSimpleItemWritable, IntWritable, NodeVectorOrSimpleWritable>
    {
        public int dimVector, k;

        IntWritable iw = new IntWritable();
        NodeVectorOrSimpleWritable nvosw = new NodeVectorOrSimpleWritable();

        @Override
        protected void setup(Mapper<NodeVectorWritable, KNNSimpleItemWritable, IntWritable, NodeVectorOrSimpleWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector",0);
            k = conf.getInt("k", 30);
            NodeVectorWritable.dimVector = dimVector;
            NodeVectorOrSimpleWritable.dimVector = dimVector;
            KNNSimpleItemWritable.numNeighbors = k;
        }

        @Override
        protected void map(NodeVectorWritable key, KNNSimpleItemWritable value, Mapper<NodeVectorWritable, KNNSimpleItemWritable, IntWritable, NodeVectorOrSimpleWritable>.Context context) throws IOException, InterruptedException {

            iw.set(key.id);
            nvosw.set(key.id, key.vector);
            context.write(iw, nvosw);

            for(int i = 0 ; i < k ; i++)
            {
                iw.set(value.neighbors[i]);
                nvosw.set(value.distances[i], value.flag_new[i]);
                context.write(iw, nvosw);
            }
        }
    }

    public static class Step1Reducer extends Reducer<IntWritable, NodeVectorOrSimpleWritable, NodeVectorWritable, NNSimpleWritable>
    {
        NodeVectorWritable nv = new NodeVectorWritable();
        NNSimpleWritable nns = new NNSimpleWritable();

        @Override
        protected void setup(Reducer<IntWritable, NodeVectorOrSimpleWritable, NodeVectorWritable, NNSimpleWritable>.Context context) {

            NodeVectorOrSimpleWritable.dimVector = context.getConfiguration().getInt("dimVector",0);
            NodeVectorWritable.dimVector = context.getConfiguration().getInt("dimVector",0);
            nns.setup();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeVectorOrSimpleWritable> values, Reducer<IntWritable, NodeVectorOrSimpleWritable, NodeVectorWritable, NNSimpleWritable>.Context context) throws IOException, InterruptedException {

            nns.setup();
            for(NodeVectorOrSimpleWritable nvosw : values)
            {
                if(nvosw.hasVector)
                {
                    nv.set(nvosw.id, nvosw.vector, false, false, false);
                }
                else
                {
                    nns.add(nvosw.id, nvosw.distance, nvosw.flag_new);
                }
            }

            context.write(nv, nns);
        }
    }

    public void runStep2(String inputPath, String outputPath, int round) throws Exception
    {
        Job job = Job.getInstance(getConf(), "4R : Round(" + round + ") : Step2");
        job.setJarByClass(NNDMR.class);

        job.setMapperClass(Step2Mapper.class);
        job.setReducerClass(Step2Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeVectorDistanceWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NNItemDistanceWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
        momb = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES).getValue();
    }

    public static class Step2Mapper extends Mapper<NodeVectorWritable, NNSimpleWritable, IntWritable, NodeVectorDistanceWritable>
    {
        public int sampleSize;

        ArrayList<Integer> old_items = new ArrayList<Integer>();
        ArrayList<Integer> new_items = new ArrayList<Integer>();

        IntWritable uw = new IntWritable();
        NodeVectorDistanceWritable nvw = new NodeVectorDistanceWritable();

        @Override
        protected void setup(Mapper<NodeVectorWritable, NNSimpleWritable, IntWritable, NodeVectorDistanceWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            NodeVectorWritable.dimVector = conf.getInt("dimVector", 0);
            NodeVectorDistanceWritable.dimVector = conf.getInt("dimVector", 0);
            sampleSize = (int)(conf.getInt("k", 30) * conf.getFloat("sample", 0.5f));
        }

        @Override
        protected void map(NodeVectorWritable key, NNSimpleWritable value, Mapper<NodeVectorWritable, NNSimpleWritable, IntWritable, NodeVectorDistanceWritable>.Context context) throws IOException, InterruptedException {

            uw.set(key.id);
            nvw.set(key.id, key.vector, Float.POSITIVE_INFINITY, false, false, false);
            context.write(uw, nvw);

            old_items.clear();
            new_items.clear();

            for(int i = 0 ; i < value.numNeighbors ; i++)
            {
                if(value.flag_new.get(i)) new_items.add(i);
                else old_items.add(i);
            }

            int size = 0;
            Collections.shuffle(new_items);
            size = new_items.size();
            while(size-->sampleSize)
            {
                new_items.remove(size);
            }
            Collections.shuffle(old_items);
            size = old_items.size();
            while(size-->sampleSize)
            {
                old_items.remove(size);
            }

            nvw.flag_reverse = false;
            for(int i = 0 ; i < value.numNeighbors ; i++)
            {
                uw.set(value.neighbors.get(i));
                nvw.distance = value.distances.get(i);
                nvw.flag_new = value.flag_new.get(i);
                if(old_items.contains(i) || new_items.contains(i)) nvw.flag_sample = true;
                else nvw.flag_sample = false;
                context.write(uw, nvw);
            }
        }
    }

    public static class Step2Reducer extends Reducer<IntWritable, NodeVectorDistanceWritable, IntWritable, NNItemDistanceWritable>
    {
        NNItemDistanceWritable nndw = new NNItemDistanceWritable();

        @Override
        protected void setup(Reducer<IntWritable, NodeVectorDistanceWritable, IntWritable, NNItemDistanceWritable>.Context context) {

            NodeVectorDistanceWritable.dimVector = context.getConfiguration().getInt("dimVector", 0);
            NNItemDistanceWritable.dimVector = context.getConfiguration().getInt("dimVector", 0);
            nndw.initialize();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeVectorDistanceWritable> values, Reducer<IntWritable, NodeVectorDistanceWritable, IntWritable, NNItemDistanceWritable>.Context context) throws IOException, InterruptedException {

            nndw.initialize();

            for(NodeVectorDistanceWritable nvw : values)
            {
                if(nvw.id == key.get())
                {
                    nndw.setVector(nvw.id, nvw.vector);
                }
                else
                {
                    nndw.add(nvw.id, nvw.vector, nvw.distance, nvw.flag_new, false, nvw.flag_sample);
                }
            }

            context.write(key, nndw);
        }
    }

    public void runStep3(String inputPath, String outputPath, int round) throws Exception
    {
        Job job = Job.getInstance(getConf(), "4R : Round(" + round + ") : Step3");
        job.setJarByClass(NNDMR.class);

        job.setMapperClass(Step3Mapper.class);
        job.setReducerClass(Step3Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeVectorDistanceWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NNItemDistanceWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
        momb = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES).getValue();
    }

    public static class Step3Mapper extends Mapper<IntWritable, NNItemDistanceWritable, IntWritable, NodeVectorDistanceWritable>
    {
        IntWritable uw = new IntWritable();
        NodeVectorDistanceWritable nvdw = new NodeVectorDistanceWritable();

        @Override
        protected void setup(Mapper<IntWritable, NNItemDistanceWritable, IntWritable, NodeVectorDistanceWritable>.Context context) {

            NodeVectorDistanceWritable.dimVector = context.getConfiguration().getInt("dimVector", 0);
            NNItemDistanceWritable.dimVector = context.getConfiguration().getInt("dimVector", 0);
        }

        @Override
        protected void map(IntWritable key, NNItemDistanceWritable value, Mapper<IntWritable, NNItemDistanceWritable, IntWritable, NodeVectorDistanceWritable>.Context context) throws IOException, InterruptedException {

            nvdw.set(value.id, value.vector, Float.POSITIVE_INFINITY, false, false, false);
            context.write(key, nvdw);

            for(int i = 0 ; i < value.numNeighbors ; i++)
            {
                nvdw.set(value.neighbors.get(i), value.vectors.get(i), value.distances.get(i), value.flag_new.get(i), false, false);
                context.write(key, nvdw);

                if(value.flag_sample.get(i))
                {
                    uw.set(value.neighbors.get(i));
                    nvdw.set(value.id, value.vector, value.distances.get(i), value.flag_new.get(i), true, false);
                    context.write(uw, nvdw);
                }
            }
        }
    }

    public static class Step3Reducer extends Reducer<IntWritable, NodeVectorDistanceWritable, IntWritable, NNItemDistanceWritable>
    {
        NNItemDistanceWritable nndw = new NNItemDistanceWritable();

        @Override
        protected void setup(Reducer<IntWritable, NodeVectorDistanceWritable, IntWritable, NNItemDistanceWritable>.Context context) {

            NodeVectorDistanceWritable.dimVector = context.getConfiguration().getInt("dimVector", 0);
            NNItemDistanceWritable.dimVector = context.getConfiguration().getInt("dimVector", 0);
            nndw.initialize();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeVectorDistanceWritable> values, Reducer<IntWritable, NodeVectorDistanceWritable, IntWritable, NNItemDistanceWritable>.Context context) throws IOException, InterruptedException {

            nndw.initialize();

            for(NodeVectorDistanceWritable nvdw : values)
            {
                if(nvdw.id == key.get())
                {
                    nndw.setVector(nvdw.id, nvdw.vector);
                }
                else
                {
                    nndw.add(nvdw.id, nvdw.vector, nvdw.distance, nvdw.flag_new, nvdw.flag_reverse, false);
                }
            }

            context.write(key, nndw);
        }
    }

    public long runStep4(String inputPath, String outputPath, int round) throws Exception
    {
        Job job = Job.getInstance(getConf(), "4R : Round(" + round + ") : Step4");
        job.setJarByClass(NNDMR.class);

        job.setMapperClass(Step4Mapper.class);
        job.setReducerClass(Step4Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeVectorOrSimpleWritable.class);

        job.setOutputKeyClass(NodeVectorWritable.class);
        job.setOutputValueClass(KNNSimpleItemWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
        momb = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES).getValue();

        return job.getCounters().findCounter(Counters.NUM_CHANGES).getValue();
    }

    public static class Step4Mapper extends Mapper<IntWritable, NNItemDistanceWritable, IntWritable, NodeVectorOrSimpleWritable>
    {
        int k;

        IntWritable uw = new IntWritable();
        NodeVectorOrSimpleWritable nvosw = new NodeVectorOrSimpleWritable();

        @Override
        protected void setup(Mapper<IntWritable, NNItemDistanceWritable, IntWritable, NodeVectorOrSimpleWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 0);
            NNItemDistanceWritable.dimVector = conf.getInt("dimVector", 0);
            NodeVectorOrSimpleWritable.dimVector = conf.getInt("dimVector", 0);
        }

        @Override
        protected void map(IntWritable key, NNItemDistanceWritable value, Mapper<IntWritable, NNItemDistanceWritable, IntWritable, NodeVectorOrSimpleWritable>.Context context) throws IOException, InterruptedException {

            nvosw.set(value.id, value.vector);
            context.write(key, nvosw);

            long counter = 0;
            for(int i = 0 ; i < value.numNeighbors ; i++)
            {
                if(!value.flag_reverse.get(i))
                {
                    nvosw.id = value.neighbors.get(i);
                    nvosw.set(value.distances.get(i), false);
                    context.write(key, nvosw);
                }

                for(int j = i + 1 ; j < value.numNeighbors ; j++)
                {
                    if(value.flag_new.get(i) || value.flag_new.get(j))
                    {
                        nvosw.set(Distances.l2(value.vectors.get(i), value.vectors.get(j)), true);
                        counter++;

                        uw.set(value.neighbors.get(i));
                        nvosw.id = value.neighbors.get(j);
                        context.write(uw, nvosw);
                        uw.set(value.neighbors.get(j));
                        nvosw.id = value.neighbors.get(i);
                        context.write(uw, nvosw);
                    }
                }
            }
        }
    }

    public static class Step4Reducer extends Reducer<IntWritable, NodeVectorOrSimpleWritable, NodeVectorWritable, KNNSimpleItemWritable>
    {
        int k, dimVector;
        float[] temp;

        NodeVectorWritable nvw = new NodeVectorWritable();
        KNNSimpleItemWritable knnsw = new KNNSimpleItemWritable();

        @Override
        protected void setup(Reducer<IntWritable, NodeVectorOrSimpleWritable, NodeVectorWritable, KNNSimpleItemWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            KNNSimpleItemWritable.numNeighbors = k = conf.getInt("k", 30);
            NodeVectorOrSimpleWritable.dimVector = conf.getInt("dimVector", 0);
            NodeVectorWritable.dimVector = conf.getInt("dimVector", 0);
            knnsw.setup();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeVectorOrSimpleWritable> values,
                              Reducer<IntWritable, NodeVectorOrSimpleWritable, NodeVectorWritable, KNNSimpleItemWritable>.Context context)
                throws IOException, InterruptedException {

            HashSet<Integer> prev = new HashSet<Integer>();
            PriorityQueue<NodeVectorDistance> pq = new PriorityQueue<NodeVectorDistance>();

            for(NodeVectorOrSimpleWritable nvosw : values)
            {
                if(nvosw.id == key.get())
                {
                    nvw.set(nvosw.id, nvosw.vector, false, false, false);
                }
                else
                {
                    if(!nvosw.flag_new) prev.add(nvosw.id);
                    pq.add(new NodeVectorDistance(nvosw.id, temp, nvosw.distance, nvosw.flag_new));
                }
            }

            NodeVectorDistance cur;
            for(int i = 0, prevId = -1 ; i < k ; i++)
            {
                do cur = pq.poll(); while(cur.id == prevId);

                knnsw.neighbors[i] = prevId = cur.id;
                knnsw.distances[i] = cur.dist;
                knnsw.flag_new[i] = cur.flag_new;
            }

            int numChanges = 0;
            for(int id : knnsw.neighbors) if(!prev.contains(id)) numChanges++;
            context.getCounter(Counters.NUM_CHANGES).increment(numChanges);

            context.write(nvw, knnsw);
        }
    }

    public void writeResult(String inputPath, String outputPath) throws Exception
    {
        Job job = Job.getInstance(getConf(), "Converting_Result");
        job.setJarByClass(NNDMR.class);

        job.setMapperClass(ResultMapper.class);
        job.setReducerClass(ResultReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);
    }

    public static class ResultMapper extends Mapper<NodeVectorWritable, KNNSimpleItemWritable, IntWritable, IntWritable>
    {
        IntWritable u = new IntWritable();
        IntWritable v = new IntWritable(-1);

        @Override
        protected void setup(Mapper<NodeVectorWritable, KNNSimpleItemWritable, IntWritable, IntWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            NodeVectorWritable.dimVector = conf.getInt("dimVector", 0);
            KNNSimpleItemWritable.numNeighbors = conf.getInt("k", 30);
        }

        @Override
        protected void map(NodeVectorWritable key, KNNSimpleItemWritable value, Mapper<NodeVectorWritable, KNNSimpleItemWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

            u.set(key.id);
            for(int val : value.neighbors)
            {
                v.set(val);
                context.write(u, v);
            }
        }
    }

    public static class ResultReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>
    {
        Text knn = new Text("");

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {

            String result = "";
            for(IntWritable v : values)
            {
                result += v.get() + " ";
            }
            knn.set(result);
            context.write(key, knn);
        }
    }
}
