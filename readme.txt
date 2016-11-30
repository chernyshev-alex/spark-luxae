email : chernyshev.alexander@gmail.com

Simple spark in-memory analytics

1. Prerequisited

maven 3.2+
java 1.8+

2. How to run

gradle test
- or -
mvn test

3. Description

src/test/java/SparkAppTest - functional  test
src/main/java/SparkApp  - solutiuon with comments
./data/example_input.txt  - quotes
./data/INSTRUMENT_PRICE_MODIFIER.csv - price modifiers file "uploaded from database"

Notes : 

  

<End>

===  Source:  programming task specification  ==========================

In the financial world we're operating on a term "financial instrument". 
You can think of it as of a collection of prices of currencies, commodities, derivatives, etc.

For the purpose of this exercise we provide you an input file with multiple time series containing prices of instruments:

-	INSTRUMENT1
-	INSTRUMENT2
-	INSTRUMENT3

File format is as follows:

<INSTRUMENT_NAME>,<DATE>,<VALUE>

For instance:

INSTRUMENT1,12-Mar-2015,12.21

TASK:

Read time series from the file provided and pass all of them to the "calculation module".

Calculation engine needs to calculate:

For INSTRUMENT1 – mean
For INSTRUMENT2 – mean for November 2014
For INSTRUMENT3 – any other statistical calculation that we can compute "on-the-fly" as we read the file (it's up to you)
For any other instrument from the input file - sum of the newest 10 elements


