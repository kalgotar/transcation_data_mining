### Association Rule Mining

Association rule mining using MapReduce framework to identify and compute confidence of frequent itemsets (2-itemsets: {a}->b and 3-itemsets: {a,b}->c where a, b, c are items in transaction.

FrequentItemsetMapper -> FrequentItemsetPartitioner -> FrequentItemsetReducer -> ComputationMapper -> ComputationReducer

#### Input
1 2 4 6 7

2 3

2 4 5

...

#### Output
{1} -> 2   &nbsp;&nbsp; &nbsp;&nbsp;    0.6666667

{1} -> 4   &nbsp;&nbsp; &nbsp;&nbsp;     0.3333334

{2,4} -> 1  &nbsp;&nbsp;&nbsp;&nbsp;     0.5

...

#### Usage

`$ bin/hadoop jar /path/rulemining.jar my.ids594.rulemining.RuleMining /hdfspath/input/ /hdfspath/temp/ /hdfspath/output/'`


