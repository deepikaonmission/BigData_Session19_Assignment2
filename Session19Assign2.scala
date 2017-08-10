/* <<<<<<<<-------- QUERIES ---------->>>>>>>>>>
Using udfs on dataframe

1. Change firstname, lastname columns into
Mr.first_two_letters_of_firstname<space>lastname

for example - michael, phelps becomes Mr.mi phelps

2. Add a new column called ranking using udfs on dataframe, where :
gold medalist, with age >= 32 are ranked as pro
gold medalists, with age <= 31 are ranked amateur
silver medalist, with age >= 32 are ranked as expert
silver medalists, with age <= 31 are ranked rookie
*/

import org.apache.spark.sql.{Row,Column,SparkSession,SQLContext}  //Explanation is already given in Assignment18.1

object Session19Assign2 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Session19Assign2")
    .config("spark.sql.warehouse.dir", "file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 19/Assignments/Assignment2")
    .getOrCreate()
  //Explanation is already given in Assignment 18.1

  val df1 = spark.sqlContext.read.option("header","true").csv("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 19/Assignments/Assignment2/Sports_data.txt")
  /* Explanation
  -> to read text file, built-in csv api is used along with option, where header is mentioned as true because file contains headers
  -> [df1 -->> sql.DataFrame] is created as and when a file is read
  */

  df1.printSchema()       //prints schema of DataFrame
  //REFER Screenshot 1 for output

  df1.show(30)            //shows data inside DataFrame in tabular format
  //REFER Screenshot 2 for output

  //<<<<<<<<<--------- Creation of case class ------------>>>>>>>>>>>
  import spark.implicits._  //to use case class import is required
  case class df1Class(firstname:String,lastname:String,sports:String,medal_type:String,age:Int,year:Int,country:String)
  //case class "df1Class" is created to provide specific data types to df1 DataFrame, where all data is received as String

  val df2=df1.map{
    case Row(
    a:String,
    b:String,
    c:String,
    d:String,
    e:String,
    f:String,
    g:String) => df1Class(firstname=a,lastname=b,sports=c,medal_type=d,age=e.toInt,year=f.toInt,country=g)
  }
  /*Explanation
  -> df1 is mapped to case class
  -> df2 -->> Dataset[Session19Assign2.df1Class] is created which now contains type specific data
  */

  df2.printSchema()  //prints Schema of DataFrame
  //REFER Screenshot 3 for output

  df2.show(30)    //shows data inside DataFrame in tabular format
  //REFER Screenshot 4 for output


  //<<<<<<<<<<<---------- QUERY 1 ------------->>>>>>>>>>>>
  /* Using udfs on dataframe
  1. Change firstname, lastname columns into
  Mr.first_two_letters_of_firstname<space>lastname
  for example - michael, phelps becomes Mr.mi phelps
  */

  //<<<<<<<<--------- Creation of UDF --------->>>>>>>>>
  def udfChangeColumns = org.apache.spark.sql.functions.udf((firstname:String,lastname:String) => {
    val twoCharsFromFirstName = firstname.substring(0,2)
    val name = "Mr."+twoCharsFromFirstName+" "+lastname
    name
  })
  /* Explanation
  -> new udf with name "udfChangeColumns" is created using def udfChangeColumns = org.apache.spark.sql.functions.udf, which accepts two parameters i.e. (firstname:String,lastname:String)
  -> val twoCharsFromFirstName = firstname.substring(0,2)
     * substring(0,2) fetches only first two characters from entire firstname field
     * and new variable twoCharsFromFirstName stores these two characters
  -> val name = "Mr."+twoCharsFromFirstName+" "+lastname
     * "Mr."+twoCharsFromFirstName+" "+lastname -->> concatenates three strings i.e. "Mr.",twoCharsFromFirstName and lastname
     * and concatenated string is stored in new variable "name"
  -> finally value of "name" is returned from udf "udfChangeColumns"
  */

  val df3 = df2.withColumn("name",udfChangeColumns($"firstname",$"lastname"))
  /* Explanation:
  -> withColumn appends a new column in newly created dataframe i.e. df3
  -> syntax of withColumn is df.withColumn("newColName",col)
  -> here call to udf is made by passing two parameters i.e. firstname and lastname
  -> and new name i.e. "name" is given to second parameter which is returned by udf
  */

  df3.show(30)    //shows data in df3 in tabular format
  //REFER Screenshot 5 for output

  val df4 = df3.drop("firstname","lastname")
  //two columns of df3 are dropped and new df4 contains only undropped fields and corresponding data
  df4.show(30)    //data is shown in tabular format
  //REFER Screenshot 6 for output

  val df5 = df4.select("name","sports","medal_type","age","year","country")
  //to interchange the position of columns, select api is used with df4
  df5.show(30)      //data is shown in tabular format
  //REFER Screenshot 7 for output



  //<<<<<<<<<<<---------- QUERY 2 ------------->>>>>>>>>>>>
  /* 2. Add a new column called ranking using udfs on dataframe, where :
  gold medalist, with age >= 32 are ranked as pro
  gold medalists, with age <= 31 are ranked amateur
  silver medalist, with age >= 32 are ranked as expert
  silver medalists, with age <= 31 are ranked rookie
  */

  def udfAddNewColumn = org.apache.spark.sql.functions.udf((medal_type:String,age:Int) => {
    if (medal_type == "gold" && age >= 32)
      "pro"
    else if (medal_type == "gold" && age <= 31)
      "amateur"
    else if (medal_type == "silver" && age >= 32)
      "expert"
    else
      "rookie"

  })
  /* Explanation:
  -> new udf "udfAddNewColumn" is created using def udfAddNewColumn = org.apache.spark.sql.functions.udf, which accepts two parameters i.e. (medal_type:String,age:Int)
  -> Inside udf i.e. in body of udf, various conditions are mentioned and while call is made to udf
     * conditions are checked and string is returned by udf "udfAddNewColumn" based on whichever condition is true
  */

  val df6 = df2.withColumn("ranking",udfAddNewColumn($"medal_type",$"age"))
  /* Explanation:    Working on df2 i.e. original data (7 fields)
  -> call to udf udfAddNewColumn is made inside withColumn by passing two parameters i.e. medal_type and age
  -> data returned by udf is given a name i.e. ranking which is appended in df6 dataframe
  */
  df6.show(30)       //data is shown in tabular format
  //REFER Screenshot 8 for output

  val df7 = df5.withColumn("ranking",udfAddNewColumn(df4.col("medal_type"),df4.col("age")))
  /* Explanation:    Working on df2 i.e. modified data (6 fields)
  -> call to udf udfAddNewColumn is made inside withColumn by passing two parameters i.e. medal_type and age
  -> data returned by udf is given a name i.e. ranking which is appended in df7 dataframe
  */
  //val df7 = df4.withColumn("ranking",udfAddNewColumn($"medal_type",$"age"))    //works similar as above
  df7.show(30)      //data is shown in tabular format
  //REFER Screenshot 9 for output



 }
