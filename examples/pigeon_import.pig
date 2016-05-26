DEFINE ST_Area edu.umn.cs.pigeon.Area;
DEFINE ST_AsHex edu.umn.cs.pigeon.AsHex;
DEFINE ST_AsText edu.umn.cs.pigeon.AsText;
DEFINE ST_Boundary edu.umn.cs.pigeon.Boundary;
DEFINE ST_Break edu.umn.cs.pigeon.Break;
DEFINE ST_Buffer edu.umn.cs.pigeon.Buffer;
DEFINE ST_Connect edu.umn.cs.pigeon.Connect;
DEFINE ST_Contains edu.umn.cs.pigeon.Contains;
DEFINE ST_ConvexHull edu.umn.cs.pigeon.ConvexHull;
DEFINE ST_Crosses edu.umn.cs.pigeon.Crosses;
DEFINE ST_Decompose edu.umn.cs.pigeon.Decompose;
DEFINE ST_Difference edu.umn.cs.pigeon.Difference;
DEFINE ST_Envelope edu.umn.cs.pigeon.Envelope;
DEFINE ST_ESRIFromWKB edu.umn.cs.pigeon.ESRIShapeFromWKB;
DEFINE ST_ESRIFromText edu.umn.cs.pigeon.ESRIShapeFromText;
DEFINE ST_Extent edu.umn.cs.pigeon.Extent;
DEFINE ST_GeomFromWKB edu.umn.cs.pigeon.GeometryFromWKB;
DEFINE ST_GeomFromText edu.umn.cs.pigeon.GeometryFromText;
DEFINE ST_GridCell edu.umn.cs.pigeon.GridCell;
DEFINE ST_GridPartition edu.umn.cs.pigeon.GridPartition;
DEFINE ST_Intersection edu.umn.cs.pigeon.Intersection;
DEFINE ST_Intersects edu.umn.cs.pigeon.Intersects;
DEFINE ST_IsEmpty edu.umn.cs.pigeon.IsEmpty;
DEFINE ST_IsValid edu.umn.cs.pigeon.IsValid;
DEFINE ST_MakeBox edu.umn.cs.pigeon.MakeBox;
DEFINE ST_MakeLine edu.umn.cs.pigeon.MakeLine;
DEFINE ST_MakeLinePolygon edu.umn.cs.pigeon.MakeLinePolygon;
DEFINE ST_MakePoint edu.umn.cs.pigeon.MakePoint;
DEFINE ST_MakePolygon edu.umn.cs.pigeon.MakePolygon;
DEFINE ST_MakeSegments edu.umn.cs.pigeon.MakeSegments;
DEFINE ST_NumPoints edu.umn.cs.pigeon.NumPoints;
DEFINE ST_Overlaps edu.umn.cs.pigeon.Overlaps;
DEFINE ST_SJPlaneSweep edu.umn.cs.pigeon.SJPlaneSweep;
DEFINE ST_Touches edu.umn.cs.pigeon.Touches;
DEFINE ST_Union edu.umn.cs.pigeon.Union;
DEFINE ST_Within edu.umn.cs.pigeon.Within;
DEFINE ST_XMin edu.umn.cs.pigeon.XMin;
DEFINE ST_XMax edu.umn.cs.pigeon.XMax;
DEFINE ST_YMin edu.umn.cs.pigeon.YMin;
DEFINE ST_YMax edu.umn.cs.pigeon.YMax;

DEFINE PBSM(dataset1, dataset2, geom1, geom2) RETURNS overlappingPairs {
  dataset1_mbrs = FOREACH $dataset1 GENERATE (*) AS tuple1, ST_Envelope($geom1) AS mbr1;
  dataset2_mbrs = FOREACH $dataset2 GENERATE (*) AS tuple2, ST_Envelope($geom2) AS mbr2;
  mbrs1 = FOREACH dataset1_mbrs GENERATE mbr1 AS mbr;
  mbrs2 = FOREACH dataset2_mbrs GENERATE mbr2 AS mbr;
  allmbrs = UNION mbrs1, mbrs2;
  gallmbrs = GROUP allmbrs ALL;
  gridInfo = FOREACH gallmbrs GENERATE ST_Extent(allmbrs.mbr) AS gridMBR,
    (INT)CEIL(SQRT((DOUBLE)(COUNT(allmbrs))/10000.0)) AS gridSize;
  dataset1xgrid = CROSS dataset1_mbrs, gridInfo;
  partitioned1 = FOREACH dataset1xgrid GENERATE *,
    FLATTEN(ST_GridPartition(tuple1.$geom1, gridMBR, gridSize)) AS cellid;
  dataset2xgrid = CROSS dataset2_mbrs, gridInfo;
  partitioned2 = FOREACH dataset2xgrid GENERATE *,
    FLATTEN(ST_GridPartition(tuple2.$geom2, gridMBR, gridSize)) AS cellid;
  allpartitioned = COGROUP partitioned1 BY (cellid, gridMBR, gridSize), partitioned2 BY (cellid, gridMBR, gridSize);
  finalResult = FOREACH allpartitioned GENERATE FLATTEN
    (ST_SJPlaneSweep(partitioned1.tuple1, partitioned2.tuple2, ST_GridCell(group.cellid, group.gridMBR, group.gridSize), partitioned1.mbr1, partitioned2.mbr2));
  $overlappingPairs = FOREACH finalResult GENERATE FLATTEN(tuple1), FLATTEN(tuple2);
};

