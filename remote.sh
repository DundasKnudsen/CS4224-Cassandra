 #!/usr/bin/env bash
  
for nc in 10 20 40
do
    for cl in "ONE" "QUORUM"
    do
        ssh xcnd50 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash run.sh 1 $nc $cl" &
        ssh xcnd51 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash run.sh 2 $nc $cl" &
        ssh xcnd52 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash run.sh 3 $nc $cl" &
        ssh xcnd53 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash run.sh 4 $nc $cl" &
        ssh xcnd54 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash run.sh 5 $nc $cl" &
        wait
        echo "Done $nc $cl..."
    done
 
done