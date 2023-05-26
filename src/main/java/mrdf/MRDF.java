package mrdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.*;
import utils.Counters;
import writables.*;

import java.io.IOException;
import java.util.*;

public class MRDF extends Configured implements Tool {

    long mor, mob, momb;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MRDF(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        long t1, t2;

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        int reducers = conf.getInt("mapreduce.job.reduces", 1);
        String inputPath = conf.get("input");
        String outputPath = conf.get("output", "result");

        String vectorPath = outputPath + ".vectors";
        conf.setInt("mapreduce.job.reduces", 0);
        fs.delete(new Path(vectorPath), true);
        splitDataset(inputPath, vectorPath);

        int blockCount = 0;
        String seedPath = outputPath + ".seed.txt";
        FSDataOutputStream out = fs.create(new Path(seedPath), true);
        RemoteIterator<LocatedFileStatus> it =  fs.listFiles(new Path(vectorPath + "/blocks"), false);
        while(it.hasNext())
        {
            String filename = it.next().getPath().toString();
            String[] tmp = filename.split("/");
            out.writeBytes(tmp[tmp.length - 1]);
            out.writeByte('\n');
            blockCount++;
        }
        out.close();
        int nLine = 1;
        int numWorkers = conf.getInt("numWorkers", 0);
        if(numWorkers > 0)
            nLine = blockCount / numWorkers;

        String splitter = "Cx2q";
        conf.set("splitter", splitter);
        conf.set("vectorPath", vectorPath);

        long numChanges = Long.MAX_VALUE;
        int numVector = conf.getInt("numVectors", 0);
        long KN = numVector * conf.getInt("k", 30);
        long threshold = (long)(KN * (double)conf.getFloat("tau", 0.01f));

        int ep = 0;
        while(numChanges > threshold) {
            String curInputPath, curOutputPath;

            int itr = 0;
            long processed = 1;
            while (processed > 0) {
                conf.setInt("itr", itr);
                if (itr == 0)
                    conf.setInt("mapreduce.job.reduces", 1);
                else
                    conf.setInt("mapreduce.job.reduces", reducers);

                curInputPath = outputPath + "." + (itr);
                conf.set("curInputPath", curInputPath);
                curOutputPath = outputPath + "." + (itr + 1);
                fs.delete(new Path(curOutputPath), true);
                t1 = System.currentTimeMillis();
                processed = dnc(seedPath, curOutputPath, ep, itr, nLine);
                t2 = System.currentTimeMillis();
                fs.delete(new Path(curInputPath), true);

                it = fs.listFiles(new Path(curOutputPath + "/divFile"), false);
                while (it.hasNext()) {
                    String filename = it.next().getPath().toString();
                    fs.rename(new Path(filename), new Path(filename.split(splitter)[0]));
                }

                System.out.printf("ep:%2d,\tit:%2d\t%15d\t%15d\t%15d\t%15d\n", ep, itr, t2 - t1, mor, mob, momb);
                if (itr == 0)
                    processed = 1;
                itr++;
            }

            curInputPath = outputPath + "." + itr;
            conf.set("curInputPath", curInputPath);
            fs.delete(new Path(outputPath + ".knn." + ep), true);
            t1 = System.currentTimeMillis();
            updateLeafBlock(seedPath, outputPath + ".knn." + ep, ep);
            t2 = System.currentTimeMillis();
            System.out.printf("ep:%2d,\tit: K\t%15d\t%15d\t%15d\t%15d\n", ep, t2 - t1, mor, mob, momb);

            fs.delete(new Path(curInputPath), true);

            fs.delete(new Path(outputPath + ".ep." + (ep+1)), true);
            t1 = System.currentTimeMillis();
            numChanges = mergeKNN(outputPath + ".knn.", outputPath + ".ep.", ep);
            t2 = System.currentTimeMillis();
            fs.delete(new Path(outputPath + ".knn." + ep), true);
            fs.delete(new Path(outputPath + ".ep." + ep), true);
            System.out.printf("ep:%2d,\tmerge:\t%15d\t%15d\t%15d\t%15d\t%15d\n", ep, t2 - t1, mor, mob, momb, numChanges);

            ep++;
        }

        String finalPath = outputPath + ".ep." + ep;
        String resultPath = outputPath + ".ep.F";
        fs.delete(new Path(resultPath), true);
        convertResult(finalPath, resultPath);
        fs.delete(new Path(finalPath), true);

        fs.delete(new Path(seedPath), true);
        fs.delete(new Path(vectorPath), true);

        return 0;
    }

    public void splitDataset(String input, String output) throws Exception {
        Job job = Job.getInstance(getConf(), "dataset split");
        job.setJarByClass(MRDF.class);

        job.setMapperClass(splitMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "vectorFile", SequenceFileOutputFormat.class, IntWritable.class, VectorIDWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(false);
    }

    public static class splitMapper extends Mapper<Object, Text, IntWritable, VectorIDWritable> {
        int dimVector;
        float[] vector;
        MultipleOutputs<IntWritable, VectorIDWritable> mos;

        IntWritable iw;
        VectorIDWritable vw;

        @Override
        protected void setup(Mapper<Object, Text, IntWritable, VectorIDWritable>.Context context) {
            VectorIDWritable.dimVector = dimVector = context.getConfiguration().getInt("dimVector", 0);
            vector = new float[dimVector];
            mos = new MultipleOutputs<>(context);

            iw = new IntWritable(0);
            vw = new VectorIDWritable();
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, VectorIDWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());

            int v = Integer.parseInt(st.nextToken());
            for(int i = 0 ; i < dimVector ; i++)
            {
                vector[i] = Float.parseFloat(st.nextToken());
            }

            iw.set(v);
            vw.set(vector);
            mos.write("vectorFile", iw, vw, "blocks/part");
        }

        @Override
        protected void cleanup(Mapper<Object, Text, IntWritable, VectorIDWritable>.Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public long dnc(String input, String output, int ep, int itr, int nLine) throws Exception {
        getConf().set("mapreduce.map.memory.mb", "6900");
        getConf().set("mapreduce.map.java.opts", "-Xmx5900m");
        getConf().set("mapreduce.reduce.memory.mb", "5000");
        getConf().set("mapreduce.reduce.java.opts", "-Xmx4000m");

        Job job = Job.getInstance(getConf(), "EP: " + ep + ", IT: " + itr);
        job.setJarByClass(MRDF.class);

        job.setMapperClass(dncMapper.class);
        job.setPartitionerClass(dncPartitioner.class);
        job.setReducerClass(dncReducer.class);

        job.setMapOutputKeyClass(DivPathWritable.class);
        job.setMapOutputValueClass(VectorIDWritable.class);

        job.setOutputKeyClass(DivPathWritable.class);
        job.setOutputValueClass(InfoWritable.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, nLine);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "divFile", SequenceFileOutputFormat.class, DivPathWritable.class, VectorIDWritable.class);
        MultipleOutputs.addNamedOutput(job, "infoFile", SequenceFileOutputFormat.class, DivPathWritable.class, InfoWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
        momb = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES).getValue();

        return job.getCounters().findCounter(Counters.NUM_PROCESSED).getValue();
    }

    public static class dncMapper extends Mapper<Object, Text, DivPathWritable, VectorIDWritable> {
        int div, leaf, itr;
        String splitter;
        String curInputPath, vectorPath;

        Configuration conf;
        SequenceFile.Reader reader;
        MultipleOutputs<DivPathWritable, VectorIDWritable> mos;

        Map<String, InfoWritable> info;

        InfoWritable infoValue;
        DivPathWritable dpw;
        VectorIDWritable vidw;
        IntWritable iw;

        Reservoir[] reservoirs;

        @Override
        protected void setup(Mapper<Object, Text, DivPathWritable, VectorIDWritable>.Context context) throws IOException, InterruptedException
        {
            conf = context.getConfiguration();
            InfoWritable.dimVector = VectorIDWritable.dimVector = conf.getInt("dimVector", 0);
            div = InfoWritable.div = Reservoir.div = conf.getInt("rho", 15);
            leaf = conf.getInt("alpha", 150000);
            itr = conf.getInt("itr", 0);
            splitter = conf.get("splitter");
            curInputPath = conf.get("curInputPath");
            vectorPath = conf.get("vectorPath");

            infoValue = new InfoWritable();
            infoValue.initialize();
            dpw = new DivPathWritable();
            vidw = new VectorIDWritable();
            iw = new IntWritable();

            FileSystem fs = FileSystem.get(conf);
            info = new HashMap<>();
            if(itr > 0)
            {
                RemoteIterator<LocatedFileStatus> it;
                it = fs.listFiles(new Path(curInputPath + "/infoFile"), false);
                while (it.hasNext())
                {
                    String filename = it.next().getPath().toString();
                    reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(filename)));
                    while (reader.next(dpw, infoValue))
                    {
                        info.put(dpw.toString(), infoValue);
                        infoValue = new InfoWritable();
                    }
                    reader.close();
                }
            }
            mos = new MultipleOutputs<>(context);

            reservoirs = new Reservoir[(int)Math.pow(div, itr)];
        }

        private int divToIndex(DivPathWritable dpw)
        {
            int ret = 0;
            for(int i = dpw.depth - 1, exp = 1 ; i > 0 ; i--, exp *= div)
            {
                ret += dpw.divisions[i] * exp;
            }
            return ret;
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, DivPathWritable, VectorIDWritable>.Context context) throws IOException, InterruptedException
        {
            Map<Integer, DivPathWritable> divInfo = new HashMap<>();
            if(itr > 0)
            {
                reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(curInputPath + "/divFile/" + value.toString())));
                while (reader.next(dpw, vidw)) {
                    divInfo.put(vidw.id, dpw);
                    dpw = new DivPathWritable();
                }
                reader.close();
            }

            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(vectorPath + "/blocks/" + value.toString())));
            while(reader.next(iw, vidw))
            {
                if(itr == 0)
                {
                    dpw.divisions[0] = (byte)0;
                    dpw.depth = (byte)1;

                    if(reservoirs[0] == null)
                        reservoirs[0] = new Reservoir(dpw);
                    reservoirs[0].update(vidw.vector);

                    vidw.set(iw.get());
                    mos.write("divFile", dpw, vidw, "divFile/" + value + splitter);
                    continue;
                }
                dpw = divInfo.get(iw.get());
                if(!info.containsKey(dpw.toString()))
                {
                    vidw.set(iw.get());
                    mos.write("divFile", dpw, vidw, "divFile/" + value + splitter);
                    continue;
                }

                infoValue = info.get(dpw.toString());
                if(infoValue.blockSize <= leaf)
                {
                    vidw.set(iw.get());
                    mos.write("divFile", dpw, vidw, "divFile/" + value + splitter);
                    continue;
                }

                context.getCounter(Counters.NUM_PROCESSED).increment(1);
                float minVal = Float.MAX_VALUE;
                int minIdx = 0;
                for(int i = 0 ; i < div ; i++)
                {
                    float dist = Distances.l2(vidw.vector, infoValue.centroids[i]);
                    if(dist < minVal)
                    {
                        minVal = dist;
                        minIdx = i;
                    }
                }

                dpw.divisions[dpw.depth] = (byte)minIdx;
                dpw.depth += 1;
                int idx = divToIndex(dpw);
                if(reservoirs[idx] == null)
                    reservoirs[idx] = new Reservoir(dpw);
                reservoirs[idx].update(vidw.vector);

                vidw.set(iw.get());
                mos.write("divFile", dpw, vidw, "divFile/" + value + splitter);
            }
            reader.close();
        }

        @Override
        protected void cleanup(Mapper<Object, Text, DivPathWritable, VectorIDWritable>.Context context) throws IOException, InterruptedException
        {
            for(Reservoir rv : reservoirs)
            {
                if(rv == null)
                    continue;

                vidw.set(rv.cnt);
                context.write(rv.dpw, vidw);

                for(int k = 0 ; k < Math.min(div, rv.cnt) ; k++)
                {
                    context.write(rv.dpw, rv.vectors[k]);
                }
            }
            mos.close();
        }
    }

    public static class dncPartitioner extends Partitioner<DivPathWritable, VectorIDWritable> {
        @Override
        public int getPartition(DivPathWritable divPathWritable, VectorIDWritable vectorIDWritable, int num)
        {
            int sum = 0;
            for(byte i = 0 ; i < divPathWritable.depth ; i++)
            {
                sum += divPathWritable.divisions[i];
            }
            return sum % num;
        }
    }

    public static class dncReducer extends Reducer<DivPathWritable, VectorIDWritable, DivPathWritable, InfoWritable>
    {
        int div;
        Random rnd;
        InfoWritable ifw = new InfoWritable();
        MultipleOutputs<DivPathWritable, InfoWritable> mos;

        @Override
        protected void setup(Reducer<DivPathWritable, VectorIDWritable, DivPathWritable, InfoWritable>.Context context) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            InfoWritable.div = div = conf.getInt("rho", 15);
            InfoWritable.dimVector = VectorIDWritable.dimVector = conf.getInt("dimVector", 0);
            ifw.initialize();
            rnd = new Random();
            mos = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(DivPathWritable key, Iterable<VectorIDWritable> values, Reducer<DivPathWritable, VectorIDWritable, DivPathWritable, InfoWritable>.Context context) throws IOException, InterruptedException
        {
            int size = 0;
            int cnt = 0;
            for(VectorIDWritable vidw : values)
            {
                if(vidw.id != -1)
                {
                    size += vidw.id;
                    continue;
                }
                if(cnt < div) {
                    ifw.set(cnt, vidw.vector);
                }
                else {
                    int tmp = rnd.nextInt(cnt+1);
                    if(tmp < div)
                    {
                        ifw.set(tmp, vidw.vector);
                    }
                }
                cnt++;
            }
            ifw.blockSize = size;
            mos.write("infoFile", key, ifw, "infoFile/part");
        }

        @Override
        protected void cleanup(Reducer<DivPathWritable, VectorIDWritable, DivPathWritable, InfoWritable>.Context context) throws IOException, InterruptedException
        {
            mos.close();
        }
    }

    public void updateLeafBlock(String input, String output, int ep) throws Exception
    {
        getConf().set("mapreduce.map.memory.mb", "1024");
        getConf().set("mapreduce.map.java.opts", "-Xmx820m");
        getConf().set("mapreduce.reduce.memory.mb", "5500");
        getConf().set("mapreduce.reduce.java.opts", "-Xmx3000m");

        Job job = Job.getInstance(getConf(), "EP: " + ep + ", IT: K");
        job.setJarByClass(MRDF.class);

        job.setMapperClass(lbMapper.class);
        job.setPartitionerClass(lbPartitioner.class);
        job.setReducerClass(lbReducer.class);

        job.setMapOutputKeyClass(DivPathWritable.class);
        job.setMapOutputValueClass(PureVectorWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NodeWritable.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 1);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
        momb = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES).getValue();
    }

    public static class lbMapper extends Mapper<Object, Text, DivPathWritable, PureVectorWritable>
    {
        String curInputPath, vectorPath;

        Configuration conf;
        SequenceFile.Reader reader;

        @Override
        protected void setup(Mapper<Object, Text, DivPathWritable, PureVectorWritable>.Context context) throws IOException, InterruptedException
        {
            conf = context.getConfiguration();
            PureVectorWritable.dimVector = VectorIDWritable.dimVector = conf.getInt("dimVector", 0);
            curInputPath = conf.get("curInputPath");
            vectorPath = conf.get("vectorPath");
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, DivPathWritable, PureVectorWritable>.Context context) throws IOException, InterruptedException
        {
            DivPathWritable dpw = new DivPathWritable();
            VectorIDWritable vidw = new VectorIDWritable();
            Map<Integer, DivPathWritable> divInfo = new HashMap<>();
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(curInputPath + "/divFile/" + value.toString())));
            while (reader.next(dpw, vidw))
            {
                divInfo.put(vidw.id, dpw);
                dpw = new DivPathWritable();
            }
            reader.close();

            IntWritable iw = new IntWritable();
            PureVectorWritable pvw = new PureVectorWritable();
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(vectorPath + "/blocks/" + value.toString())));
            while(reader.next(iw, vidw))
            {
                dpw = divInfo.get(iw.get());
                pvw.set(iw.get(), vidw.vector);
                context.write(dpw, pvw);
            }
            reader.close();
        }
    }

    public static class lbPartitioner extends Partitioner<DivPathWritable, PureVectorWritable>
    {
        @Override
        public int getPartition(DivPathWritable divPathWritable, PureVectorWritable vectorIDWritable, int num)
        {
            int sum = 0;
            for(byte i = 0 ; i < divPathWritable.depth ; i++)
            {
                sum += divPathWritable.divisions[i];
            }
            return sum % num;
        }
    }

    public static class lbReducer extends Reducer<DivPathWritable, PureVectorWritable, IntWritable, NodeWritable>
    {
        int k, sampleSize, dimVector, leaf;
        float threshold;

        int[] ids;
        float[][] vectors;

        IntWritable iw = new IntWritable();
        NodeWritable nw = new NodeWritable();

        @Override
        protected void setup(Reducer<DivPathWritable, PureVectorWritable, IntWritable, NodeWritable>.Context context) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            dimVector = PureVectorWritable.dimVector = conf.getInt("dimVector", 0);
            k = conf.getInt("k", 30);
            sampleSize = k;
            threshold = 0.001f;
            leaf = conf.getInt("alpha", 150000);

            ids = new int[leaf];
            vectors = new float[leaf][dimVector];
        }

        @Override
        protected void reduce(DivPathWritable key, Iterable<PureVectorWritable> values, Reducer<DivPathWritable, PureVectorWritable, IntWritable, NodeWritable>.Context context) throws IOException, InterruptedException
        {
            int cnt = 0;
            for(PureVectorWritable pvw : values)
            {
                ids[cnt] = pvw.id;
                System.arraycopy(pvw.vector, 0, vectors[cnt], 0, dimVector);
                cnt++;
            }

            if(cnt > 10000)
            {
                GraphHeap graph = NNDescentSingle.build(vectors, cnt, k, 50, sampleSize, Distances::l2, threshold);
                for (int i = 0; i < cnt; i++)
                {
                    iw.set(ids[i]);
                    for (int j = 0; j < k; j++)
                    {
                        nw.set(ids[graph.neighbors[i][j]], graph.distances[i][j], true);
                        context.write(iw, nw);
                    }
                }
            }
            else
            {
                for(int i = 0 ; i < cnt ; i++)
                {
                    PriorityQueue<NodeWritable> pq = new PriorityQueue<>(Comparator.reverseOrder());
                    for(int j = 0 ; j < cnt ; j++)
                    {
                        if(i == j) continue;
                        float dist = Distances.l2(vectors[i], vectors[j]);
                        if(pq.size() == k)
                        {
                            if(dist < pq.peek().distance)
                            {
                                pq.poll();
                                pq.add(new NodeWritable(ids[j], dist, true));
                            }
                        }
                        else
                        {
                            pq.add(new NodeWritable(ids[j], dist, true));
                        }
                    }

                    iw.set(ids[i]);
                    for(NodeWritable ndw : pq)
                    {
                        context.write(iw, ndw);
                    }
                }
            }
        }
    }

    public long mergeKNN(String nodeInput, String knnInput, int ep) throws Exception
    {
        getConf().set("mapreduce.map.memory.mb", "1024");
        getConf().set("mapreduce.map.java.opts", "-Xmx820m");
        getConf().set("mapreduce.reduce.memory.mb", "6000");
        getConf().set("mapreduce.reduce.java.opts", "-Xmx4800m");

        Job job = Job.getInstance(getConf(), "merging trees");
        job.setJarByClass(MRDF.class);

        job.setReducerClass(knnReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(KnnWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(nodeInput + ep), SequenceFileInputFormat.class, nodeMapper.class);
        if(ep > 0)
            MultipleInputs.addInputPath(job, new Path(knnInput + ep), SequenceFileInputFormat.class, knnMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(knnInput + (ep+1)));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
        momb = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES).getValue();

        return job.getCounters().findCounter(Counters.NUM_CHANGES).getValue();
    }

    public static class knnMapper extends Mapper<IntWritable, KnnWritable, IntWritable, NodeWritable>
    {
        int k;
        NodeWritable nw = new NodeWritable();

        @Override
        protected void setup(Mapper<IntWritable, KnnWritable, IntWritable, NodeWritable>.Context context) throws IOException, InterruptedException {
            k = KnnWritable.numNeighbors = context.getConfiguration().getInt("k", 30);
        }

        @Override
        protected void map(IntWritable key, KnnWritable value, Mapper<IntWritable, KnnWritable, IntWritable, NodeWritable>.Context context) throws IOException, InterruptedException {
            for(int i = 0 ; i < k ; i++)
            {
                nw.set(value.neighbors[i], value.distances[i], false);
                context.write(key, nw);
            }
        }
    }

    public static class nodeMapper extends Mapper<IntWritable, NodeWritable, IntWritable, NodeWritable>
    {
        @Override
        protected void map(IntWritable key, NodeWritable value, Mapper<IntWritable, NodeWritable, IntWritable, NodeWritable>.Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class knnReducer extends Reducer<IntWritable, NodeWritable, IntWritable, KnnWritable>
    {
        int k;
        KnnWritable knn = new KnnWritable();

        @Override
        protected void setup(Reducer<IntWritable, NodeWritable, IntWritable, KnnWritable>.Context context) throws IOException, InterruptedException
        {
            k = KnnWritable.numNeighbors = context.getConfiguration().getInt("k", 30);
            knn.setup();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeWritable> values, Reducer<IntWritable, NodeWritable, IntWritable, KnnWritable>.Context context) throws IOException, InterruptedException
        {
            PriorityQueue<NodeWritable> pq = new PriorityQueue<>();
            for(NodeWritable nw : values)
            {
                pq.add(new NodeWritable(nw.id, nw.distance, nw.flag));
            }

            int newCnt = 0;
            int idx = 0;
            int prev = -1;
            NodeWritable nw;
            while(idx < k)
            {
                do
                {
                    nw = pq.poll();
                } while(nw != null && nw.id == prev);
                if(pq.size() == 0) break;
                prev = nw.id;
                if(nw.flag)
                    newCnt++;
                knn.set(idx, nw.id, nw.distance);
                idx++;
            }
            while(idx < k)
            {
                knn.set(idx, -1, Float.MAX_VALUE);
                idx++;
                newCnt++;
            }

            context.getCounter(Counters.NUM_CHANGES).increment(newCnt);
            context.write(key, knn);

            super.reduce(key, values, context);
        }
    }

    public void convertResult(String inputPath, String outputPath) throws Exception
    {
        Job job = Job.getInstance(getConf(), "Final Result");
        job.setJarByClass(MRDF.class);

        job.setMapperClass(resultMapper.class);
        job.setReducerClass(resultReducer.class);

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

    public static class resultMapper extends Mapper<IntWritable, KnnWritable, IntWritable, IntWritable>
    {
        int k = 0;
        IntWritable kw = new IntWritable();
        IntWritable vw = new IntWritable();

        @Override
        protected void setup(Mapper<IntWritable, KnnWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = KnnWritable.numNeighbors = conf.getInt("k", 30);
        }

        @Override
        protected void map(IntWritable key, KnnWritable value, Mapper<IntWritable, KnnWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

            kw.set(key.get());
            for(int i = 0 ; i < k ; i++)
            {
                vw.set(value.neighbors[i]);
                context.write(kw, vw);
            }
        }
    }

    public static class resultReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>
    {
        Text knn = new Text("");

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException
        {
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
