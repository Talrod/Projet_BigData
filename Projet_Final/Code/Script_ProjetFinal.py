# Importation des bibliothèques et des modules

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf 
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import datediff, to_date


# Instanciation du client Spark Session

conf = SparkConf().set('spark.driver.host','127.0.0.1')
sc = SparkContext(master='local', appName='myAppName',conf=conf)

spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName("CreateTable")\
                    .getOrCreate()

# Création des tables
Author = sc.parallelize([["07890","Jean Paul Sartre"], 
                         ["05678","Pierre de Ronsard"]])\
           .toDF(("aid", "name"))
Author.show()
Author.createOrReplaceTempView("Author_sql")


book = sc.parallelize([["0001","L'existantialisme est un humanisme","Philisophie"], 
                       ["0002","Huis clos. Suivi de Les Mouches","Philosophie"], 
                       ["0003","Mignonne allons voir si la rose","Poeme"], 
                       ["0004","Les Amours","Poeme"]])\
         .toDF(("bid", "title","category"))
book.show()
book.createOrReplaceTempView("book_sql")


Student = sc.parallelize([["S15","toto","Math"], 
                          ["S16","popo","Eco"], 
                          ["S17","fofo","Mecanique"]])\
            .toDF(("sid", "sname","dept"))
Student.show()
Student.createOrReplaceTempView("Student_sql")


write = sc.parallelize([["07890","0001"], 
                        ["07890","0002"], 
                        ["05678","0003"], 
                        ["05678","0003"]])\
          .toDF(("aid", "bid"))
write.show()
write.createOrReplaceTempView("write_sql")


borrow  = sc.parallelize([["S15","0003","02-01-2020","01-02-2020"], 
                          ["S15","0002","13-06-2020",None], 
                          ["S15","0001","13-06-2020","13-10-2020"], 
                          ["S16","0002","24-01-2020","24-01-2020"],
                          ["S17","0001","12-04-2020","01-07-2020"]])\
            .toDF(("sid", "bid","checkout-time","return-time"))
borrow.show()
borrow.createOrReplaceTempView("borrow_sql")

# 1 - Trouver les titres de tous les livres que l'étudiant sid='S15' a emprunté
##DSL
borrow.join(book, "bid")\
     .select("sid","title")\
     .filter(col("sid") == "S15")\
     .show()

##SQL
spark.sql("""SELECT sid, title from borrow_sql
        left join book_sql
        on borrow_sql.bid=book_sql.bid
        WHERE borrow_sql.sid="S15"
         """).show()


# 2 - Trouver les titres de tous les livres qui n'ont jamais été empruntés par un étudiant
##DSL
book.join(borrow, "bid", how="left")\
     .select("title")\
     .filter(col("checkout-time").isNull())\
     .show()

##SQL
spark.sql("""SELECT title from book_sql
        left join borrow_sql
        on book_sql.bid=borrow_sql.bid
        WHERE borrow_sql.bid IS NULL
         """).show()


# 3 - Trouver tous les étudiants qui ont emprunté le livre bid=’0002’
##DSL
Student.join(borrow, "sid")\
     .select("sname","bid")\
     .filter(col("bid") == "0002")\
     .show()

##SQL
spark.sql("""SELECT sname, bid from Student_sql
        left join borrow_sql
        on Student_sql.sid=borrow_sql.sid
        WHERE borrow_sql.bid="0002"
         """).show()


# 4 - Trouver les titres de tous les livres empruntés par des étudiants en informatique (département informatique)
##DSL
book.join(borrow, "bid")\
    .join(Student,"sid")\
    .select("dept","title")\
    .filter(col("dept") == "Mecanique")\
    .show()

##SQL
spark.sql("""SELECT dept as Departement, title as Titre from Student_sql
        inner join borrow_sql
        on Student_sql.sid=borrow_sql.sid
        inner join book_sql
        on book_sql.bid=borrow_sql.bid
        WHERE Student_sql.dept = "Mecanique"
         """).show()


# 5 - Trouver les étudiants qui n’ont jamais emprunté de livre
##DSL
Student.join(borrow, "sid", how="left")\
       .select("sname")\
       .filter(col("bid").isNull())\
       .show()

##SQL
spark.sql("""SELECT sname from Student_sql
        left join borrow_sql
        on Student_sql.sid=borrow_sql.sid
        WHERE borrow_sql.bid IS NULL
         """).show()


# 6 - Déterminer l’auteur qui a écrit le plus de livres
##DSL
Author.join(write, "aid", how="left")\
      .distinct()\
      .groupBy("name")\
      .agg(F.count(col("bid")).alias("nombre"))\
      .sort("nombre", ascending=False)\
      .limit(1)\
      .show()

##SQL
spark.sql("""SELECT name, count(distinct bid) as nombre from Author_sql
        left join write_sql
        on Author_sql.aid=write_sql.aid
        GROUP BY name
        ORDER BY nombre DESC LIMIT 1
         """).show()


# 7 - Déterminer les personnes qui n’ont pas encore rendu les livres
##DSL
Student.join(borrow, "sid", how="left")\
       .select("sname")\
       .filter(col("return-time").isNull())\
       .show()

##SQL
spark.sql("""SELECT sname from Student_sql
        left join borrow_sql
        on Student_sql.sid=borrow_sql.sid
        WHERE borrow_sql.`return-time` IS NULL
         """).show()


# 8 - Création nouvelle colonne
##DSL
borrow = borrow.withColumn("duree",
                             datediff(to_date("return-time","dd-MM-yyyy"),
                                      to_date("checkout-time","dd-MM-yyyy")))\
               .withColumn("duree",when(col("duree")>=90,1).otherwise(0))
borrow.show()

##SQL
spark.sql('''select sid, bid, `checkout-time`, `return-time`, 
                case 
                    when days >= 90 then 1
                    else 0
                end as duree
            from (select *, DATEDIFF(TO_DATE(`return-time`,"dd-MM-yyyy"), TO_DATE(`checkout-time`,"dd-MM-yyyy")) as days
            from borrow_sql)''').show()

borrow.write.csv("contention/borrow.csv")


# 9 - Déterminer les livres qui n’ont jamais été empruntés
##DSL
book.join(borrow, "bid", how="left")\
     .select("title")\
     .filter(col("checkout-time").isNull())\
     .show()

##SQL
spark.sql("""SELECT title from book_sql
        left join borrow_sql
        on book_sql.bid=borrow_sql.bid
        WHERE borrow_sql.bid IS NULL
         """).show()
