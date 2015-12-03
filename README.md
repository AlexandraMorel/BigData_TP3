#BigData_TP3
## Assignment 3

### II.Introduction
Importation of the CSV file.
Creation of the Crime class and the Crimes DataFrame.

```
kinit
/usr/hdp/current/spark-client/bin/spark-shell
import java.util.Date
import java.sql.Date
import java.text.ParseException
import java.text.SimpleDateFormat
case class Crime(cdatetime:java.sql.Date,address:String,district:Int,beat:String,grid:Int,
            crimdescr:String,ucr_ncic_code:Int,latitude:Float,longitude:Float)
val file = sc.textFile("/res/spark_assignment/crimes.csv").
			mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else
			iter}
val crimes = file.map(line => {val l = line.split(",")
			Crime(new java.sql.Date((new SimpleDateFormat("MM/dd/yy").parse(l(0))).getTime()), l(1), l(2).toInt, l(3), l(4).toInt, l(5), l(6).toInt, l(7).toFloat, l(8).toFloat)})

val crimesDF = crimes.toDF
crimesDF.printSchema
```

### III.Practice
#### Part 1
Queries using RDD computation.
The crime that happens the most in Sacramento.
```
val maxCrimesQuery = crimes.map(_.crimdescr).max
```
Result : maxCrimesQuery: String = WARRANT SERVED - I RPT

The 3 days with the highest crime count.
```
val DaysMostCrimes = crimes.map(_.cdatetime).countByValue().toSeq.sortWith(_._2 > _._2).take(3)
```
Queries using Data Frame.
```
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
crimesDF.registerTempTable("DFcrimes")
```
The crime that happens the most in Sacramento.
```
val maxCrimesQueryDF = sqlContext.sql("SELECT MAX(crimdescr) FROM DFcrimes")
```
The 3 days with the highest crime count.
```
val DaysMostCrimesDF = sqlContext.sql("SELECT cdatetime FROM DFcrimes GROUP BY cdatetime ORDER BY COUNT(cdatetime) DESC LIMIT 3")
```
The average of each crime per day.
```
val AvgCrimesPerDayDF = sqlContext.sql("SELECT cdatetime, AVG(ucr_ncic_code) AS avg FROM DFcrimes GROUP BY cdatetime, crimedescr")
```

#### Part 2
Export a CSV file that contains the average of crimes per day per districts.
```
val file = "/tmp/querySaved.csv"
FileUtil.fullyDelete(new File(file))

val avgCrimes = crimes.map(_.crimdescr).countByValue/crimes.map(_.district).countByValue
val districtCrimes = crimes.reduceByKey {case (avgCrimes,cdatetime) => avgCrimes + cdatetime}.
	map { case (key, value) => Array(key, value).mkString(",") }

districtCrimes.saveAsTextFile(file)
```
