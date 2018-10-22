appNo=$1

EXEC=8
MEM=4404
CORES=4
FRAC=0.6
RATIO=4

for EXEC in 16 24 32
do
  CORES=$(expr 32 / $EXEC)
  MEM=$(expr 4404 \* 8 / $EXEC)
  FRAC=0.6
  RATIO=2
  for RATIO in 2 
  do
#    for FRAC in 0.4 0.5 0.6 0.7 0.8
    for CORES in 2
    do
      bash m4xlargethoth-run.sh $appNo $MEM $CORES $EXEC $FRAC $RATIO 
      ((appNo++))
    done
  done
done

<<C
#for CORES in 1 2 3 4
#do
  bash m4xlargethoth-run.sh $appNo $MEM $CORES $EXEC $FRAC $RATIO 
#  ((appNo++))
#done
C
