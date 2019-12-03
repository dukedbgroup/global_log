appNo=$1

EXEC=8
MEM=4404
CORES=2
FRAC=0.6
RATIO=4

CACHE="MEMORY_AND_DISK"
#for PART in 240 210 180 150 120 90
#do
#  cd ~/global_log/sparklogprocessor; bash m4physical.sh $appNo $PART $CACHE
#  ((appNo++))
#done



for EXEC in 32 
do
#  CORES=$(expr 32 / $EXEC)
  MEM=$(expr 4404 \* 8 / $EXEC)
  FRAC=0.6
  RATIO=2
  for TCORES in 32 64
  do
   CORES=$((TCORES/EXEC))
   if [ $CORES -lt 1 -o $(expr $CORES \* $EXEC) -gt 64 ]
   then
    echo "Skipping $EXEC times $CORES"
    continue
   fi
   for FRAC in 0.2 0.4 0.6 0.8
   do
    for RATIO in 1 3 5 7
    do
      cd ~/global_log/sparklogprocessor; bash m4xlargethoth-run.sh $appNo $MEM $CORES $EXEC $FRAC $RATIO 
      ((appNo++))
    done
   done
  done
done

<<C
for CORES in 1 2 3 4
do
  bash m4xlargethoth-run.sh $appNo $MEM $CORES $EXEC $FRAC $RATIO 
  ((appNo++))
done
C
