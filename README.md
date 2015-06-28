# Comparative Study of Frameworks for Parallel Processing of Graphs

This repository contains my final paper, presented on the [Federal Technological University of Paraná](http://utfpr.edu.br/) as a requirement to obtain my graduation degree in System Analysis and Development. The complete paper can be viewed [here](https://github.com/tayllan/my-final-paper/blob/master/final_paper.pdf) (in Portuguese, though). The advisor was [Francisco Pereira Junior](http://lattes.cnpq.br/9599087600537534).

It also contains some codes used in it:

* [PageRank](https://github.com/tayllan/my-final-paper/tree/master/codes/HadoopPageRank) and [ShortestPath](https://github.com/tayllan/my-final-paper/tree/master/codes/HadoopShortestPath) for the [Apache Hadoop](https://github.com/apache/hadoop) framework;
* [PageRank](https://github.com/tayllan/my-final-paper/tree/master/codes/GiraphPageRank) and [ShortestPath](https://github.com/tayllan/my-final-paper/tree/master/codes/GiraphShortestPath) for the [Apache Giraph](https://github.com/apache/giraph) framework;
* [PageRank](https://github.com/tayllan/my-final-paper/tree/master/codes/HamaPageRank) and [ShortestPath](https://github.com/tayllan/my-final-paper/tree/master/codes/HamaShortestPath) for the [Apache Hama](https://github.com/apache/hama) framework;
* [PageRank](https://github.com/tayllan/my-final-paper/tree/master/codes/SparkPageRank) and [ShortestPath](https://github.com/tayllan/my-final-paper/tree/master/codes/SparkShortestPath) for the [Apache Spark](https://github.com/apache/spark) framework;

These codes were put here not to be executed "has they are" (too much configuration here and there to run these frameworks), but rather as a reference material. If you have any questions, feel free to send me an email at tayllanburigo@gmail.com.

## Comparative Study

These algorithms were executed in the four frameworks as a way to compare the efficiency of the frameworks. The comparison measures were **total time of execution** and **memory consumption**. They were executed in two computing environments:

* a standalone machine with 7.7 GBs of RAM, processor Intel Core i5-2400M, and running Ubuntu 13.10 64-bit.
* a cluster consisting of 3 virtual machines, each one with 4 GBs of RAM, processor Intel Xeon e7-4830 and running Ubuntu 14.04 64-bit.

As an example of the results collected, the following table shows the running time in seconds for the frameworks executing the PageRank algorithm, with 10 and 20 iterations, for each of the used datasets:

| Frameworks | # of Iterations | web-Stanford.txt | meu-grafo.txt | web-Google.txt |
|:----------:|:---------------:|:----------------:|:-------------:|:--------------:|
|   Hadoop   |        10       |           326.35 |        330.60 |         570.97 |
|            |        20       |           638.01 |        647.32 |       1,129.26 |
|    Spark   |        10       |           269.22 |        283.65 |         729.45 |
|            |        20       |           479.42 |        480.18 |       1,353.69 |
|   Giraph   |        10       |            44.67 |         45.14 |          62.43 |
|            |        20       |            49.87 |         53.33 |          86.02 |
|    Hama    |        10       |           106.61 |        111.62 |         257.61 |
|            |        20       |           189.53 |        190.56 |         476.68 |

More results (including memory consumption) and more information on the algorithms and datasets can be seen in the [final paper](https://github.com/tayllan/my-final-paper/blob/master/final_paper.pdf) (in Portuguese, though).
