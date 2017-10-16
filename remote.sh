 #!/usr/bin/env bash
  
for nc in 10 20 40
do
    for cl in "ONE" "QUORUM"
    do
        ssh xcnd50 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash load_data.sh /home/stuproj/cs4224k/CS4224-Cassandra/data/data-files > log/$nc-$cl-load-data.txt"
        ssh xcnd50 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash main.sh 1 $nc $cl > log/1-$nc-$cl-log.txt" &
        ssh xcnd51 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash main.sh 2 $nc $cl > log/2-$nc-$cl-log.txt" &
        ssh xcnd52 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash main.sh 3 $nc $cl > log/3-$nc-$cl-log.txt" &
        ssh xcnd53 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash main.sh 4 $nc $cl > log/4-$nc-$cl-log.txt" &
        ssh xcnd54 "cd /home/stuproj/cs4224k/CS4224-Cassandra && bash main.sh 5 $nc $cl > log/5-$nc-$cl-log.txt" &
        wait
        echo "Done $nc $cl..."
    done
 
done