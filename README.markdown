Pigeon
======

Pigeon is a spatial extension to Pig that allows it to process spatial data.
All functionalities in Pigeon are introduced as user-defined functions (UDFS)
which makes it unobtrusive and allows it to work with your existing systems.
All the spatial functionality is supported by [ESRI Geometry API]
(https://github.com/Esri/geometry-api-java)
a native Java open source library for spatial functionality licensed under
[Apache Public License](http://www.apache.org/licenses/LICENSE-2.0.html).


Our target is to have something like [PostGIS](http://postgis.net/) but for Pig
instead of PostgreSQL. We use the same function names to make it easier for
existing users to use Pigeon. Here is an example the computes the union of all
ZIP codes in each city. 

    zip_codes = LOAD 'zips' AS (zip, city, geom);
    zip_by_city = GROUP zip_codes BY city;
    zip_union = FOREACH zip_by_city
        GENERATE group AS city, ST_Union(geom);


Data types
==========
Currently, Pig does not support the creation of custom data types. This is not
the best thing for Pigeon because we wanted to have our own data type (Geometry) similar to
PostGIS. As a work around, we use the more generic type `bytearray` as our main
data type. All conversions happen from `bytearray` to `Geometry` and vice-verse on
the fly while the function is executed. If a function expects an input of type
Geometry, it receives a `bytearray` and converts it `Geometry`. If the output is
of type `Geometry`, it computes the output, converts it to `bytearray`, and returns
that `bytearray` instead. This is a little bit cumbersome, but the Pig team is
able to add custom data types so that we have a cleaner extension.


How to compile
==============
Pigeon requires ESRI Geometry API to compile. You need to
download a recent version of the library and add it to the classpath of your Java compiler
to be able to compile the code. Of course you also need Pig classes to be available
in the classpath. The current code is tested against ESRI Geometry API 1.0 and Pig 0.11.1.
Currently you have to do the compilation manually but we are planning to create
an ANT build file to automate the compilation. Once you compile the code, you
can create a jar file out of it and [REGISTER](http://pig.apache.org/docs/r0.11.1/basic.html#register) it in your Pig scripts.


How to use
==========
To use Pigeon in your Pig scripts, you need to [REGISTER](http://pig.apache.org/docs/r0.11.1/basic.html#register) the jar file in your Pig
script. Then you can use the spatial functionality in your script as you cdo with
normal functionality. Here are some simple examples on how to use Pigeon.

Let's say you have a trajectory in the form (latitude, longitude, timestamp). We need to
for a Linestring out of this trajectory when points in this linestring are sorted
by timestamp.


    points = LOAD 'trajectory.tsv' AS (time: datetime, lat:double, lon:double);
    s_points = FOREACH points GENERATE ST_MakePoint(lat, lon) AS point, time;
    points_by_time = ORDER s_points BY time;
    points_grouped = GROUP points_by_time ALL;
    lines = FOREACH points_grouped GENERATE ST_AsText(ST_MakeLine(points_by_time));
    STORE lines INTO 'line';


Contribution
============
Pigeon is open source and licensed under the Apache open source license. Your
contribution is highly welcome and appreciated. Here is a simple guideline of
how to contribute.

1. Clone your own copy of the source code or fork the project in github.
2. Pickup an issue from the list of issues associated with the project.
3. Write a test case for the new functionality and make sure it fails.
4. Fix the code so that the test case succeeds.
5. Make sure that all existing tests still pass.
6. Revise all your changes and add comments whenever needed. Make sure you
  don't make any unnecessary changes (e.g., reformatting).
7. Submit a pull request if you are a github user or send a patch if you aren't.
8. We will revise the submitted patch and merge it with the code when done.

When writing the test, keep in mind that we are not testing Pig or ESRI Geometry API. We are
testing Pigeon which is a wrapper around ESRI Geometry API. For example, you don't have to test
all the special cases of polygons if implementing an intersection function.
All what we want is to make sure that you call the right function in ESRI Geometry API and
return the output in the correct format. 

Mainly, we need to support as many as possible of the
[functions](http://postgis.net/docs/manual-1.4/ch08.html) provided by PostGIS.
