# MapReduceApproaches

Description:

It is MapReduce program to run it on Hadoop, we can run it on AWS EMR.

The Program is to compute the Co-occurence Matrix using 2 approaches:

1. Implementation of Pairs and Strips approach to compute the co-occurrence matrix where the word-pair frequency is maintained.
2. Implementation of Pairs and Strips approach to compute the co-occurrence matrix where the word-pair relative frequency is maintained.

Requirements to run the program:

Java 7
Maven
Hadoop
AWS EMR

Steps to run the program:

1.Create “BigData" jar file by using maven clean and then maven verify in build command.

2.Upload the jar in Hue account using website.

3.Create a folder in the files named “input”and upload all the input files

4.Download the jar file in to the Hadoop server by using “hadoop fs -copyToLocal /user/BigData.jar ~/" command.

5.To run the specific task, execute the specific command mentioned below:

Task 1.1 Pairs:
hadoop jar BigData.jar Task1_1_Pairs /user/input/ /user/output/task1_1_pairs

Task 1.1 Stripes: 
hadoop jar BigData.jar Task1_1_Stripes /user/input/ /user/s3764267/output/task1_1_stripes

Task 2.1 Pairs:
hadoop jar BigData.jar Task1_2_Pairs /user/input/ /user/output/task1_2_pairs

Task 2.1 Stripes:
hadoop jar BigData.jar Task1_2_Stripes /user/input/ /user/output/task1_2_stripes
