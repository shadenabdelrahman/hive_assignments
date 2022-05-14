Create database assign2;
!hadoop fs -mkdir -p /songs_assign4/assign4;
create external table assign2.songs_extern4(
    artist_id string,
     artist_latitude string,
     artist_location string,
     artist_longitude string,
     duration string,
     num_songs string,
    song_id string,
     title string
     )
     Partitioned by (artist_name string, year string)
     row format delimited
     fields terminated by ','
     lines terminated by '\n'
     location 'hdfs://namenode:8020/songs_assign4';
!hadoop fs -put songs.csv /songs_assign4;
Select * from assign4.songs_extern4;
Alter table assign4.songs_extern4 add partition(artist_name=' Gob',year='2007')
Location 'hdfs://namenode:8020/songs_assign4/assign4';
!hadoop fs -put songs.csv /songs_assign4/assign4;
!hadoop fs –ls /songs_assign4/assign4;
describe formatted assign2.songs_extern4;
Select * from assign2.songs_extern4;
create table staging3 (
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
lines terminated by '\n';
load data  local inpath 'songs.csv' into table staging3;
Insert overwrite table songs_extern4 partition (artist_name , year)
select artist_id,
artist_latitude,
artist_location,
artist_longitude,
artist_name,	
duration,
num_songs,
song_id,	
title,	
year 
From staging3 
;
drop table assign2.songs_extern4;
Select * from assign2.songs_ extern4;
Insert overwrite table assign2.songs_extern4 partition (artist_name , year)
select artist_id,
artist_latitude,
artist_location,
artist_longitude,
artist_name,	
duration,
num_songs,
song_id,	
title,	
year 
From staging3 
;
SELECT * FROM assign2.SONGS_EXTERN4;
drop table assign2.songs_EXTERN4;
create table assign2.songs_extern4(
    artist_id string,
     artist_latitude string,
     artist_location string,
     artist_longitude string,
     duration string,
     num_songs string,
     song_id string,
     title string
     )
     Partitioned by (year string,artist_name string)
     row format delimited
     fields terminated by ','
     lines terminated by '\n';


Insert overwrite table assign2.songs_extern4 partition (year='2007', artist_name)
select artist_id,
artist_latitude,
artist_location,
artist_longitude,
artist_name,	
duration,
num_songs,
song_id,	
title
From staging3 WHERE YEAR='2007'
;







