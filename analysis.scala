
import org.apache.spark.sql.ForeachWriter 
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.streaming.ProcessingTime
import java.util.HashMap
import scala.collection.mutable.WrappedArray
import Array._
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.commons.net.util.SubnetUtils
import java.util.UUID._
import java.time.LocalDate
import java.util.Calendar

sql("set spark.sql.caseSensitive=true")
// import the data from s3 location as csv 
var pops = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("s3n://pops.csv")
pops = pops.withColumn("age",pops("Age").cast(IntegerType)).drop("Age")


pops = pops.filter("age is not null")
pops = pops.filter($"Sex"!=="All").groupBy($"Sex",$"age").agg(sum("2013"),sum("2014"),sum("2015"),sum("2016")).sort(asc("Sex"),asc("age")) // removed geography column to take total population 
pops.show(false)

pops.createOrReplaceTempView("ale")
%sql
select * from ale 

//we use the useful tools of zeppelin (the spark visualization tools to lay the population-age distibutions of different years and genders on top of each other and observe differences)

/*
 
in 2013 we see that there are more males than females under the age of 25 for every single age, however we see that after the age of 36 the females are consistently more. 
This observation is generally true for all years observed (2013,2014,2015,2016) however we can see that as we move into the future the males are able to be more than the females until the age of 28 (28 in 2016 vs 25 in 2013) and also females start being more than males at 39 in 2016 vs at 36 in 2013
So overall that means that men are becoming able to live longer than they previously could, while women are not improving their life expectancy at the same rate.
 
In addition if we ignore the gender for a second we observe that the age distribution for each year although similar in shape, is clearly shifting to the right as we move into the future. That is until the age of 76 after which population numbers are almost the same for every year. 
That means that everybody keeps on living as the previous years :)

Generally, across both genders and across the 4 years of observation, we see this persistent shape of the distribution which is interesting to explore. 
The number of people born in the early 2000s is lower, throughout the 90s (as we were approaching the new millenium?) we saw a decreasing birth rate (and after that an increasing - pessimism around changing millenia?), also late 70s we saw a decrease in births . Prior to the 60s until the 30s we see same birth rate apart from this peculiar peak in the number of births seemingly having happended after the end of the second world war. Alot of happiness for winning!

Of course that would be the intepretation if nobody died along the way from their birth until now. That is not necesserilly true so it could be that the number of births in the early 2000s and late 70s were the same generally as the other years however many people from these 2 eras didnt make it till 2013. (we dont have data before that to know the distribution of previous years). However since as we said before this distribution is shifting to the right we can assume that the anomalies are a result of changing birthrates. 

Finally, it could be that the completely opposite thing happened! For example it could be that the 70s was NOT a low birth decade rather that the 60s as well as the 80s were incredibly fertile and prosperous! In the 60s the hippie movement could have contributed to alot of births as well as in the 80s the rising income per capita. 

*/ // this udf convert age column to binary   
val over65 = udf((value:Int) => value match {
    case val1 => {
        var reg = 0
        if (val1 > 65){
            reg = 1
        }
        Some(reg)
    }
})
//redefine data set , start fresh 
var pops = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("s3n://pops.csv")
pops = pops.withColumn("age",pops("Age").cast(IntegerType)).drop("Age")
pops = pops.filter("age is not null")
pops = pops.withColumn("over65",over65(pops("age")))
pops = pops.filter($"Sex"==="All").drop("age","Sex").groupBy($"Geography",$"over65").agg(sum("2013"),sum("2014"),sum("2015"),sum("2016")).sort(asc("Geography"))

pops = pops.groupBy($"Geography").pivot("over65").agg(sum("sum(2013)"),sum("sum(2014)"),sum("sum(2015)"),sum("sum(2016)"))
case class proportions(Geography:String,pr2013:Double,pr2014:Double,pr2015:Double,pr2016:Double)
// this calculates proportions for every geography and year 
var props = pops.map(x=>proportions(x.get(0).asInstanceOf[String],x.get(5).asInstanceOf[Long].toDouble/(x.get(1).asInstanceOf[Long].toDouble+x.get(5).asInstanceOf[Long].toDouble),x.get(6).asInstanceOf[Long].toDouble/(x.get(2).asInstanceOf[Long].toDouble+x.get(6).asInstanceOf[Long].toDouble),x.get(7).asInstanceOf[Long].toDouble/(x.get(3).asInstanceOf[Long].toDouble+x.get(7).asInstanceOf[Long].toDouble),x.get(8).asInstanceOf[Long].toDouble/(x.get(4).asInstanceOf[Long].toDouble+x.get(8).asInstanceOf[Long].toDouble)))

// while exploring which regions show the highest proportions of over 65 people it becomes clear that all of them are NEXT TO THE WATER and are small towns. it seems like people like to retire next to the sea in quite places but do not like to be industrious there when younger , probably because of a small economy / market. 

// in addition it is clear that the lowest proportion of over 65 people are in central London. Clearly that is the place for workers and more active people. We also see oxford, nottingham, manchester being up there in the least again being towns that are active in the global economy. 

// regarding the biggest change over time, we see that scotland specifically is getting alot of over 65 people as well as wales. Also cardiff, glasgow, falkirk are getting many young people, cities that are also in scotland. 

// clearly younger and older are not moving where they used to move (london and south england respectively) rather to scotland and wales. Could it be that people are leaving England because of Brexit?


// new line of code
