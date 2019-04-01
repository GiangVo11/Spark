// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

// COMMAND ----------

// First initialize SparkSession object by default it will available in shells as spark
val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("Spark CSV Reader")
        .getOrCreate;

// COMMAND ----------

// We will make use of the open-sourced StackOverflow dataset. We've cut down each dataset to just 10K line.
val dfTags = spark.read.format("csv").option("header", "true").load("/FileStore/tables/question_tags_10K.csv")


// COMMAND ----------

//To visually inspect some of the data points from our dataframe, we call the method show(10) which will print only 10 line items to the console.
dfTags.show(10)

// COMMAND ----------

// To show the dataframe schema which was inferred by Spark, you can call the method printSchema() on the dataframe dfTags. 
dfTags.printSchema()

// COMMAND ----------

//DataFrame Query: select columns from a dataframe. To select specific columns from a dataframe, we can use the select() method and pass in the columns which we want to select
 dfTags.select("id", "tag").show(10)

// COMMAND ----------

// DataFrame Query: filter by column value of a dataframe
// To find all rows matching a specific column value, we can use the filter() method of a dataframe. For example, let's find all rows where the tag column has a value of php.
dfTags.filter("tag == 'php'").show(10)

// COMMAND ----------

// DataFrame Query: count rows of a dataframe
// To count the number of rows in a dataframe, we can use the count() method. As an example, let's count the number of php tags in our dataframe dfTags.We've chained the filter() and count() methods.
println(s"Number of php tags=${dfTags.filter("tag=='php'").count()}")
//We've prefixed the s at the beginning of our println statement.Prepending s to any string literal allows the usage of variables directly in the string. With the s interpolator, any name that is in scope can be used within a string.The s interpolator knows to insert the value of the name variable at this location in the string
//We also used the dollar sign $ to refer to our variable. Any arbitrary expression can be embedded in ${}.


// COMMAND ----------

//DataFrame Query: SQL like query
//We can query a dataframe column and find an exact value match using the filter() method. In addition to finding the exact value, we can also query a dataframe column's value using a familiar SQL 'like' clause
dfTags.filter("tag like 's%'").show(10)


 

// COMMAND ----------

// DataFrame Query: Multiple filter chaining
dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)

// COMMAND ----------

//DataFrame Query: Multiple filter chaining
// find all tags whose value starts with letter s and then only pick id 25 or 108
dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)

// COMMAND ----------

//DataFrame Query: SQL IN clause
dfTags.
filter("id in (25,108)")
.filter("tag like's%'").
show(10)

// COMMAND ----------

// DataFrame Query: SQL Group By
// find out how many rows match each tag in our dataframe dfTags
println("Group by tag value")
dfTags.groupBy("tag").count()show(10)

// COMMAND ----------

// DataFrame Query: SQL Group By with filter
//only display tags that have more than 5 matching rows.
dfTags.groupBy("tag").count().filter("count > 5").show(10)

// COMMAND ----------

// DataFrame Query: SQL order by
  dfTags.groupBy("tag").count().filter("count > 5").orderBy("tag").show(10)

// COMMAND ----------

// DataFrame Query: Cast columns to specific data type
val dfQuestionsCSV = spark
 .read
 .option("header", "true")// Use first line of all files as header
 .option("inferSchema", "true")// Automatically infer data types
 .option("dateFormat","yyyy-MM-dd HH:mm:ss")
 .csv("/FileStore/tables/questions_10K.csv")
 .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")
 dfQuestionsCSV.printSchema()
dfQuestionsCSV.show(10)

// COMMAND ----------

// Although we've passed in the inferSchema option, Spark did not fully match the data type for some of our columns. Column closed_date is of type string and so is column owner_userid and answer_count.There are a few ways to be explicit about our column data types and for now we will show how to explicitly using the cast feature for the date fields.
val dfQuestions = dfQuestionsCSV.select(
 dfQuestionsCSV.col("id").cast("integer"),
 dfQuestionsCSV.col("creation_date").cast("timestamp"),
 dfQuestionsCSV.col("closed_date").cast("timestamp"),
 dfQuestionsCSV.col("deletion_date").cast("date"),
 dfQuestionsCSV.col("score").cast("integer"),
 dfQuestionsCSV.col("owner_userid").cast("integer"),
 dfQuestionsCSV.col("answer_count").cast("integer")
 )
dfQuestions.printSchema()
dfQuestions.show(10)
//All our columns for the questions dataframe now seem sensible with columns id, score, owner_userid and answer_count mapped to integer type, columns creation_date and closed_date are of type timestamp and deletion_date is of type date.

// COMMAND ----------

// DataFrame Query: Operate on a sliced dataframe
//slice our dataframe dfQuestions with rows where the score is greater than 400 and less than 410.
   val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()
  dfQuestionsSubset.show()

// COMMAND ----------

// DataFrame Query: Join on explicit columns.But, what if the column to join to had different names? In such a case, you can explicitly specìfy the column from each dataframe on which on join
  dfQuestionsSubset
    .join(dfTags, dfTags("id") === dfQuestionsSubset("id"))
    .show(10)

// COMMAND ----------

// DataFrame Query: Join
  dfQuestionsSubset.join(dfTags, "id").show(10)

// COMMAND ----------

//DataFrame Query: Join and select columns
//can chain the select() method after the join() in order to only display certain columns.
dfQuestionsSubset.join(dfTags,"id").select("owner_userid", "tag", "creation_date", "score").show(10)

// COMMAND ----------

//Spark supports various types of joins namely: inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti

// COMMAND ----------

dfQuestionsSubset
    .join(dfTags, Seq("id"), "inner")
    .show(10)

// COMMAND ----------

// DataFrame Query: Left Outer Join
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "left_outer")
    .show(10)

// COMMAND ----------

// DataFrame Query: Right Outer Join
  dfTags
    .join(dfQuestionsSubset, Seq("id"), "right_outer")
    .show(10)

// COMMAND ----------

// DataFrame Query: Distinct
  dfTags
    .select("tag")
    .distinct()
    .show(10)

// COMMAND ----------

// Another example

// COMMAND ----------

// Sample data
//Employees table has a nullable column. To express it in terms of statically typed Scala, one needs to use Option type.
val employees=sc.parallelize(Array[(String, Option[Int])](
  ("Rafferty", Some(31)), ("Jones", Some(33)), ("Heisenberg", Some(33)), ("Robinson", Some(34)), ("Smith", Some(34)), ("Williams", null)
)).toDF("LastName", "DepartmentID")

employees.show()


// COMMAND ----------

//Department table does not have nullable columns, type specification could be omitted.
val departments = sc.parallelize(Array(
  (31, "Sales"), (33, "Engineering"), (34, "Clerical"),
  (35, "Marketing")
)).toDF("DepartmentID", "DepartmentName").show()

// COMMAND ----------

// inner join
employees.join(departments,Seq("DepartmentID"),"inner").show()

// COMMAND ----------

// the same as the "inner join"
employees.join(departments,"DepartmentID").show()

// COMMAND ----------

// Left outer join is a very common operation, especially if there are nulls or gaps in a data
employees.join(departments,Seq("DepartmentID"),"left_outer").show()

// COMMAND ----------

employees.join(departments,Seq("DepartmentID"),"right_outer").show()

// COMMAND ----------

//Join expression, slowly changing dimensions and non-equi join
val products = sc.parallelize(Array(
  ("steak", "1990-01-01", "2000-01-01", 150),
  ("steak", "2000-01-02", "2020-01-01", 180),
  ("fish", "1990-01-01", "2020-01-01", 100)
)).toDF("name", "startDate", "endDate", "price")

products.show()

// COMMAND ----------

//There are two products only: steak and fish, price of steak has been changed once. Another table consists of product orders by day:
val orders = sc.parallelize(Array(
  ("1995-01-01", "steak"),
  ("2000-01-01", "fish"),
  ("2005-01-01", "steak")
)).toDF("date", "product")

orders.show()

// COMMAND ----------

orders
  .join(products, $"product" === $"name" && $"date" >= $"startDate" && $"date" <= $"endDate")
  .show()
//This technique is very useful, yet not that common. It could save a lot of time for those who write as well as for those who read the code.

// COMMAND ----------

val df = sc.parallelize(Array(
  (0), (1), (1)
)).toDF("c1")

df.show()
df.join(df, "c1").show()



// COMMAND ----------


