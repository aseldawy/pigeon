SPig
====

SPig is a spatial extension to Pig that allows it to process spatial data.
All functionalities in SPig are introduced as user-defined functions (UDFS)
which makes it unobtrusive and allows it to work with your existing systems.
All the spatial functionality is supported by Java Topology Suite
[JTS](http://www.vividsolutions.com/jts/)
a native Java open source library for spatial functionality licensed under LGPL.


Our target is to have something like [PostGIS](http://postgis.net/) but for Pig
instead of PostgreSQL. We use the same function names to make it easier for
existing users to use SPig. Here is an example the computes the union of all
ZIP codes in each city. 
    zip_codes = LOAD 'zips' AS (zip, city, geom);
    zip_by_city = GROUP zip_codes BY city;
    zip_union = FOREACH zip_by_city
        GENERATE group AS city, ST_Union(geom);


Data types
==========
Currently, Pig does not support the creation of custom data types. This is not
good for SPig because we wanted to have our own data type (Geometry) similar to
PostGIS. As a work around, we use the more generic type `bytearray` as our main
data type. All conversions happen from `bytearray` to `Geometry` and vice-verse on
the fly while the function is executed. If a function expects an input of type
Geometry, it receives a `bytearray` and converts it `Geometry`. If the output is
of type `Geometry`, it computes the output, converts it to `bytearray`, and returns
that `bytearray` instead. This is a little bit cumbersome, but the Pig team is
able to add custom data types so that we have a cleaner extension.


Contribution
============
SPig is open source and licensed under the Apache open source license. Your
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

When writing the test, keep in mind that we are not testing Pig or JTS. We are
testing SPig which is a wrapper around JTS. For example, you don't have to test
all the special cases of polygons if implementing an intersection function.
All what we want is to make sure that you call the right function in JTS and
return the output in the correct format. 