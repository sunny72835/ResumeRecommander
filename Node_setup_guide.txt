Datanode AMI ID: ami-0c481f8ab5632701f Image Name: Datanode-image
Namenode AMI ID: ami-0c27685906c173598 Image Name: Namenode-image
(We have kept visibility to public. The steps followed to create these AMIs are similer to programming assignment.)

Steps to run the program:
1. Create one instance from Namenode image and 3 instances from Datanode image.
On Name node:
2. Upload your key to folder ~/.ssh/key.pem
3. Create ~/.ssh/config file with following content (Remove if already exist):
Host namenode
	HostName <Public DNS of Namenode>
	User ec2-user
	IdentityFile ~/.ssh/key.pem
Host datanode1
	HostName <Public DNS of Datanode1>
	User ec2-user
	IdentityFile ~/.ssh/key.pem
Host datanode2
	HostName <Public DNS of Datanode2>
	User ec2-user
	IdentityFile ~/.ssh/key.pem
Host datanode3
	HostName <Public DNS of Datanode3>
	User ec2-user
	IdentityFile ~/.ssh/key.pem
4. Run following two commands to resolve permissions:
	chmod 400 ~/.ssh/key.pem
	chmod 400 ~/.ssh/config
5. Run following commands to transfer files to datanodes (If files exist on datanodes, you will have to login to each node and delete them):
	scp ~/.ssh/key.pem ~/.ssh/config datanode1:~/.ssh
	scp ~/.ssh/key.pem ~/.ssh/config datanode2:~/.ssh
	scp ~/.ssh/key.pem ~/.ssh/config datanode3:~/.ssh
6. Run the following commands to start Hadoop.	
	$HADOOP_HOME/sbin/start-all.sh
7. Run following commands to delete previous results if exist and disable safe mode:
	hadoop dfsadmin -safemode leave
	hdfs dfs -rm /result/*
	hdfs dfs -rmdir /result
8. Upload resume_dataset.csv, ResumeRecommender.jar, input.txt to ~ folder. (We have provided necessary java file to create jar)
9. Use below command to move resume_dataset to hdfs if not exist:
	hdfs dfs -copyFromLocal /home/ec2-user/resume_dataset.csv /ResumeRecommenderinput
10. Run below command to run the program from ~ directory:
	hadoop jar ResumeRecommender.jar ResumeRecommender /ResumeRecommenderinput/resume_dataset.csv /result input.txt
	(The last line of this command will show duration in miliseconds, the program took to execute.)
11. To see the output:
	hdfs dfs -cat /result/*
(Before re-running the code, execute step-7)	
