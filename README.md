# MRDF
- MRDF is an approximate k-NN graph construction method for large-scale and high-dimensional dataset.
- Given a set of vectors and a positive integer k, MRDF constructs a k-NN graph whose edgeset overlaps the edgeset of ground-truth as much as possible.
- It runs parellel in a cluster, exploiting Hadoop Mapreduce.
<br>

## Run

### Build

```
cd <path-to-MRDF>
mvn install
```

### Execution
```
hadoop jar <BUILD_JAR_FILE> mrdf.MRDF -DnumVectors=<#Vectors> -DdimVector=<Dimension> -Dinput=<INPUT_DATASET> [options]
```

### Parameters

```
-Doutput   File name for the output graph (default: result)

-Dk        Number of neighbors (default: 30)

-DM        Upper bound of the subset size (MRDF only) (default: 150000)

-Drho      Multiway dividing factor (MRDF only) (default: 15)

-Dtau      Early termination (MRDF only) (default: 0.01)

-Dsample   Sampling rate (NNDMR only) (default: 0.5)
 
-Det       Early termination (NNDMR only) (default: 0.001)
```

- example
```
hadoop jar mrdf-1.0.jar mrdf.MRDF -DnumVectors=1000000 -DdimVector=128 -Dinput=SIFT1M -Doutput=mrdf -Dk=50 -DM=300000 -Drho=10 -Dtau=0.001 
```
```
hadoop jar mrdf-1.0.jar nndmr.NNDMR -DnumVectors=1193514 -DdimVector=50 -Dinput=Glove1M -Doutput=nndmr -Dk=40 -Dsample=0.7 -Det=0.005
```
<br>

## Input

### Datasets

| Dataset | Description | Dimension | #Vectors | Source |
| --- | --- | --- | --- | --- |
| SIFT1M | Flickr images with the scale-invariant feature transform (SIFT) descriptor | 128 | 1,000,000 | [irisa.fr](http://corpus-texmex.irisa.fr/) |
| Glove1M | Global vectors for word representation in web dataset | 50 | 1,193,514 | [nlp.stanford.edu](https://nlp.stanford.edu/projects/glove/) |
| SUSY5M | Kinematic properties measured by the particle detectors in the accelerator, produced by monte-carlo simulation | 18 | 5,000,000 | [ics.uci.edu](https://archive.ics.uci.edu/ml/datasets/SUSY) |
| Deep1B | Vectors represented by inferencing the ImageNet dataset through the GoogleNet model | 96 | 1,000,000,000 | [skoltech.ru](http://sites.skoltech.ru/compvision/noimi/) |

### Format

MRDF requires a plain text file as an input, and each vector must be combined with the node number, which starts from 0.

Following example shows a input text file of, four vectors in 5-dimension:
```
0 -0.12936 0.23527 0.96521 -0.17349 -0.56396
1 0.44724 0.54932 -0.10395 0.79839 -0.35423
2 -0.24564 -0.68472 0.39478 0.79378 0.16332
3 -0.56302 0.49287 0.30291 0.20391 -0.20193
```
