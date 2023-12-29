# Exam project

Big Data course (81932), University of Bologna.

To run MapReduce job:
- hadoop jar BDE-mr-Ragazzi.jar MultipleJobs /user/aravaglia/exam_lr/accidents.csv /user/aravaglia/exam_lr/mr/output1 /user/aravaglia/exam_lr/mr/output2

To run Spark job:
- spark2-submit --executor-cores 3 --class Main BDE-spark-Ragazzi.jar


