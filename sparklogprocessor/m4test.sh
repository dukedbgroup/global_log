appNo=$1 
path=~/global_log/sparklogprocessor; 
#1011 kmeans relm best: 11 mins
#bash m4xlargethoth-run.sh $appNo 2202 2 16 0.7 4
bash m4xlargethoth-run.sh $appNo 4404 2 8 0.1 1
((appNo++))
bash m4xlargethoth-run.sh $appNo 4404 2 8 0.2 1
((appNo++))
bash m4xlargethoth-run.sh $appNo 4404 2 8 0.3 1
((appNo++))
bash m4xlargethoth-run.sh $appNo 4404 2 8 0.4 1
((appNo++))
bash m4xlargethoth-run.sh $appNo 4404 2 8 0.5 1
((appNo++))
bash m4xlargethoth-run.sh $appNo 4404 2 8 0.6 1
((appNo++))
<<sort-utility
#180
bash m4xlargethoth-run.sh $appNo 4404 6 8 0.13 2
((appNo++)) #181
bash m4xlargethoth-run.sh $appNo 2202 3 16 0.13 2
((appNo++)) #182
bash m4xlargethoth-run.sh $appNo 1468 1 24 0.2 1
((appNo++)) #183
bash m4xlargethoth-run.sh $appNo 1101 1 32 0.2 1
sort-utility
<<kmeans-utility
#176
bash m4xlargethoth-run.sh $appNo 4404 5 8 0.73 4
((appNo++)) #177
bash m4xlargethoth-run.sh $appNo 2202 2 16 0.7 4
((appNo++)) #178
bash m4xlargethoth-run.sh $appNo 1468 1 24 0.68 4
((appNo++)) #179
bash m4xlargethoth-run.sh $appNo 1101 1 32 0.6 3
kmeans-utility
<<svm-utility
#172
bash m4xlargethoth-run.sh $appNo 4404 6 8 0.6 2
((appNo++)) #173
bash m4xlargethoth-run.sh $appNo 2202 3 16 0.58 2
((appNo++)) #166
bash m4xlargethoth-run.sh $appNo 1468 2 24 0.58 2
((appNo++)) #175
bash m4xlargethoth-run.sh $appNo 1101 1 32 0.56 2
svm-utility
<<wc-utility
#164
bash m4xlargethoth-run.sh $appNo 4404 6 8 0.2 1
((appNo++)) #165
bash m4xlargethoth-run.sh $appNo 2202 3 16 0.2 1
((appNo++)) #166
bash m4xlargethoth-run.sh $appNo 1468 2 24 0.2 1
((appNo++)) #167
bash m4xlargethoth-run.sh $appNo 1101 2 32 0.2 1
wc-utility
<<pr-exhaustive
cd $path; bash m4xlargethoth-run.sh $appNo 2202 1 16 0.5 3
((appNo++)) #pr-relm
cd $path; bash m4xlargethoth-run.sh $appNo 2202 1 16 0.45 5
((appNo++)) #pr-gaussian
cd $path; bash m4xlargethoth-run.sh $appNo 2202 1 16 0.4 3
pr-exhaustive
<<SVM
#1480
bash m4xlargethoth-run.sh $appNo 2202 3 16 0.54 2
((appNo++)) #1476
bash m4xlargethoth-run.sh $appNo 2202 4 16 0.59 2
((appNo++)) #1471
bash m4xlargethoth-run.sh $appNo 1468 2 24 0.58 2
((appNo++)) #1470
bash m4xlargethoth-run.sh $appNo 1101 1 32 0.5 2
((appNo++)) #1470
bash m4xlargethoth-run.sh $appNo 1101 2 32 0.6 2
SVM
<<Pagerank
#1479
bash m4xlargethoth-run.sh $appNo 4404 1 8 0.43 3 
#((appNo++)) #1475
#bash m4xlargethoth-run.sh $appNo 4404 2 8 0.3 3
#((appNo++)) #1478
#bash m4xlargethoth-run.sh $appNo 2202 1 16 0.24 5
Pagerank
<<WordCount
#1473
bash m4xlargethoth-run.sh $appNo 1101 2 32 0.2 1
WordCount
<<Sort-512
#1469
bash m4xlargethoth-run.sh $appNo 1101 2 32 0.2 1
((appNo++)) #8
bash m4xlargethoth-run.sh $appNo 1101 1 32 0.2 1
((appNo++)) #1471
bash m4xlargethoth-run.sh $appNo 1468 2 24 0.2 1
Sort-512
<<Kmeans
#1461
bash m4xlargethoth-run.sh $appNo 4404 4 8 0.72 4
((appNo++)) #1462
bash m4xlargethoth-run.sh $appNo 2202 2 16 0.68 4
((appNo++)) #1463
bash m4xlargethoth-run.sh $appNo 4404 1 8 0.8 5
((appNo++)) #1464
bash m4xlargethoth-run.sh $appNo 4404 5 8 0.8 6
((appNo++)) #1465
bash m4xlargethoth-run.sh $appNo 2202 1 16 0.76 5
Kmeans
<<LHS
bash m4xlargethoth-run.sh $appNo 4404 4 8 0.6 7
((appNo++))
bash m4xlargethoth-run.sh $appNo 2202 1 16 0.4 3
((appNo++))
bash m4xlargethoth-run.sh $appNo 1468 2 24 0.2 5
((appNo++))
bash m4xlargethoth-run.sh $appNo 1101 2 32 0.8 7
LHS
