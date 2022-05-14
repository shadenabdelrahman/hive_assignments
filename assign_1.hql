create database assign6;
Describe database extended assign6;
Create database assign4_loc 
Location '/hp_db1/assign4_loc';
Use assign6;
create table assign3_intern_tab (
  eid int,
  ename string,
  age int,
  jobtype string,
  storeid int,
  storelocation string,
  salary bigint,
  yrsofexp int
)
row format delimited
fields terminated by ','
lines terminated by '\n';
Describe formatted assign3_intern_tab;
load data local inpath 'employee.csv' into table assign3_intern_tab;
!hadoop fs  -mkdir /shaden3;
!hadoop fs  -put employee.csv /shaden3;
load data inpath '/shaden3/employee.csv'  into table assign6.assign3_intern_tab;
Use assign4_loc;
create external table assign3_extern_tab (
  eid int,
  ename string,
  age int,
  jobtype string,
  storeid int,
  storelocation string,
  salary bigint,
  yrsofexp int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 'hdfs://namenode:8020/shaden3';
Describe formatted assign4_loc.assign3_extern_tab;
Use assign6;
Drop table assign3_intern_tab;
Use assign4_loc;
Drop table assign3_extern_tab;
Use assign6;
create table assign3_intern_tab (
  eid int,
  ename string,
  age int,
  jobtype string,
  storeid int,
  storelocation string,
  salary bigint,
  yrsofexp int
)
row format delimited
fields terminated by ','
lines terminated by '\n';
Use assign4_loc;
create external table assign3_extern_tab (
  eid int,
  ename string,
  age int,
  jobtype string,
  storeid int,
  storelocation string,
  salary bigint,
  yrsofexp int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 'hdfs://namenode:8020/shaden3';
Describe formatted assign3_extern_tab;
Describe formatted assign6.assign3_intern_tab;
Use assign6;
create table staging5(
  eid int,
  ename string,
  age int,
  jobtype string,
  storeid int,
  storelocation string,
  salary bigint,
  yrsofexp int
)
row format delimited
fields terminated by ','
lines terminated by '\n';
load data local inpath 'employee.csv' into table staging5;
insert into assign6.assign3_intern_tab select * from assign6.staging5;
!hadoop fs -ls / hdfs://namenode:8020/user/hive/warehouse/assign6.db/assign3_intern_tab;
!hadoop fs -ls hdfs://namenode:8020/shaden3; 
!wc -l songs.csv;
create table songs_tab7(
artist_id string,
artist_latitude string,
artist_location string,
artist_longitude string,
artist_name string,	
duration string,
num_songs string,
song_id	 string,	
title string,	
year string
)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';
load data local inpath 'songs.csv' into table songs_tab7;
select * from songs_tab7 limit 10;
Select count (*) from songs_tab7;
!hadoop fs -mkdir /songs2;
create external table assign6.songs_extern_tab2(
artist_id string,
artist_latitude string,
artist_location string,
artist_longitude string,
artist_name string,	
duration string,
num_songs string,
song_id	 string,	
title string,	
year string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 'hdfs://namenode:8020/songs2';
!hadoop fs -put songs.csv /songs2;
select * from assign4_loc.songs_extern_tab2 limit 5;
CREATE TEMPORARY TABLE assign6.employee_tmp (
 id int,
 name string,
 age int,
 gender string)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';
alter table assign6.songs_extern_tab2 rename to assign4_loc.songs_extern_tab2;
Describe formatted assign3_extern_tab2;
