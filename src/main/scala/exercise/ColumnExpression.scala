package exercise
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/* Expression
  Read the movies and select 2 column
  create abother column summing profit movies = US_Gross + Wordwide
  Select all COMEDY movies with IMDB rating above 6
*/

object ColumnExpression extends App {
  val spark = SparkSession.builder()
    .appName("moviesExtractor")
    .config("spark.master", "local")
    .getOrCreate()

  val movies = spark.read
    .option("inferSchema", "true")
    .json("src/main/ressources/main/movies.json")


  val movies2Column = movies.selectExpr("Distributor", "IMDB_Rating")
  //val movies2ColumnV2 = movies.select(Col("Distributor"), Col("IMDB_Rating"))

  //movies2Column.show(5)

  movies.show()

  val sumColumn = movies.selectExpr("Distributor", "IMDB_Rating", "US_Gross + Worldwide_Gross")

  //sumColumn.show()


  //val aboveSix = movies.filter("IMDB_Rating > 6").selectExpr("Distributor", "IMDB_Rating")

  //aboveSix.show()





  /* Agregation
  sum up ALL the profits of all the movies in the df
  count how many distinct directors we have
  show the mean and the standrad deviation of us gross revenur for the movies
  compute the average imdb rating and the average US gross revenue PER DIRECTOR
 */

  //val sumUp = movies.select(sum(col("Worldwide_Gross")))

  //val distinctDirectors = movies.select(count_distinct(col("Director")))


  val average = movies.groupBy("Director").agg(mean(col("Worldwide_Gross")))


}
