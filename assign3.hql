create table event_tab2(
artist string,
auth string,
firstName string,
gender string,
itemInSession string,
lastName string,
length string,
level string,
location string,
method string,
page string,
registration string,
sessionId string,
song string,
status string,
ts string,
userAgent string,
userId string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';
load data local inpath 'events.csv' into table event_tab2;
Select userId, song, sessionId, last_value(song)
over(partition by sessionId order by itemInSession ROWS BETWEEN UNBOUNDED
PRECEDING AND UNBOUNDED following), first_value(song)over(partition by sessionId
order by itemInSession ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) 
from event_tab2 limit 50;
SELECT userId,count(distinct song), RANK() OVER  (Order BY COUNT(distinct song) DESC) FROM
event_tab2 group by userId;
SELECT userId,count(distinct song), Row_number() OVER  (Order BY COUNT(distinct song) DESC)
FROM event_tab2 group by userId;
SELECT COUNT(song) FROM event_tab2 GROUP BY location, artist
GROUPING SETS ((location,artist),location,());
SELECT COUNT(song) FROM event_tab2 GROUP BY location, artist
GROUPING SETS ((location,artist),location, artist, ());
select userId,song ,ts from event_tab2 
order by userId,song, ts;
select userId,song ,ts from event_tab2 
cluster by userId, song, ts;




