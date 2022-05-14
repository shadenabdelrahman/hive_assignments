Create database assign2;
!hadoop fs -mkdir /songs_assign4;
create external table assign4.songs_extern4(
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