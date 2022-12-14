# StackOverFlow-Project

## Data Source

https://archive.org/download/stackexchange/stackoverflow.com-Badges.7z<br>
https://archive.org/download/stackexchange/stackoverflow.com-Posts.7z<br>
https://archive.org/download/stackexchange/stackoverflow.com-Users.7z<br>

## Schema and The Entity Relationship Diagram

https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede

## Questions 

1. What is the percentage of questions that have been answered over the years?

2. What is the reputation and badge count of users across different tenures on StackOverflow?

3. What are 10 of the “easier” gold badges to earn?

4. Which day of the week has most questions answered within an hour?

## Setting up Google Cloud Data Proc Cluster

https://cloud.google.com/dataproc/docs/guides/setup-project

## Steps for Execution in Data Proc

1. Download the data into Local file system
```bash
  wget <link>
  7z e <file-name>
```
Note: 
Installation of 7zip (if not exists)
```bash
  sudo apt-get install p7zip-full
```

2. Move the data to HDFS
```bash
 hadoop fs -put <source-path> <hdfs-destination-path>
```
3. Upload the Jar File

4. Submitting the spark job
```bash
spark-submit --packages com.databricks:spark-xml_2.12:0.15.0 --class <class-name> <jar-file-location> <args>
```

## Note

Few of the outputs shown are the result of processing partial data.

The initial input files are split into parts using :
```bash
split -a <Suffix_length> -d -l <No_of_records_per_split_file> <File_to_Split> <Prefix_for_new_files>
```
