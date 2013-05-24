REGISTER spig.jar;
REGISTER jts-1.8.jar;

IMPORT 'spig_import.pig';

points = LOAD 'trajectory.tsv' AS (type, time: datetime, lat:double, lon:double);
s_points = FOREACH points GENERATE ST_MakePoint(lat, lon) AS point, time;
points_by_time = ORDER s_points BY time;
points_grouped = GROUP points_by_time ALL;
lines = FOREACH points_grouped GENERATE ST_AsText(ST_MakeLine(points_by_time));
STORE lines INTO 'line';
