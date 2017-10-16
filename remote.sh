 #!/usr/bin/env bash
  
for nc in 10 20 40
do
    for cl in "ONE" "QUORUM"
    do
        ssh xcnd50 "bash /home/stuproj/cs4224k/CS4224-Cassandra/run.sh 1 $nc $cl" &
        ssh xcnd51 "bash /home/stuproj/cs4224k/CS4224-Cassandra/run.sh 2 $nc $cl" &
        ssh xcnd52 "bash /home/stuproj/cs4224k/CS4224-Cassandra/run.sh 3 $nc $cl" &
        ssh xcnd53 "bash /home/stuproj/cs4224k/CS4224-Cassandra/run.sh 4 $nc $cl" &
        ssh xcnd54 "bash /home/stuproj/cs4224k/CS4224-Cassandra/run.sh 5 $nc $cl" &
        wait
        echo "Done $nc $cl..."
    done
 
done