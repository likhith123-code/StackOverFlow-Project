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

## Steps for Execution

1. Download the data into Local file system
```bash
  wget <link>
  7z e <file-name>
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

The Outputs shown are the result of processing partial data as the complete data is too vast to process. Hence a couple of split files are used to process.

The initial input files are split into parts using :
<br>

```split -a <Suffix_length> -d -l <No_of_records_per_split_file> <File_to_Split> <Prefix_for_new_files>
```
