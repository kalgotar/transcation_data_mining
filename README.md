### Association Rule Mining

Association rule mining using MapReduce framework to identify and compute confidence of frequent itemsets (2-itemsets: {a}->b and 3-itemsets: {a,b}->c where a, b, c are items in transaction.

FrequentItemsetMapper -> FrequentItemsetPartitioner -> FrequentItemsetReducer -> ComputationMapper -> ComputationReducer

#### Input
1 2 5 4 6 7 
2 3
2 4 5
...

#### Output
{1} -> 2 	0.6666667
{1} -> 4 	0.3333334
{2,4} -> 1	0.5
...

#### How to use

$ bin/hadoop jar /home/pavan/Desktop/rulemining.jar my.ids594.rulemining.RuleMining /your_hdfspath/input/ /your_hdfspath/temp/ /your_hdfspath/output/


