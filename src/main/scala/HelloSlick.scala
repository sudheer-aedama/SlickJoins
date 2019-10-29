import slick.driver.H2Driver.api._
import slick.sql.SqlStreamingAction

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, _ }

// The main application
object HelloSlick extends App {
  val db = Database.forConfig("h2mem1")
  try {
    val suppliers: TableQuery[Suppliers] = TableQuery[Suppliers]
    val coffees: TableQuery[Coffees] = TableQuery[Coffees]

    locally { // initialize database
      val createSchemaAction: DBIO[Unit] = (suppliers.schema ++ coffees.schema).create

      val insertSuppliersAction: DBIO[Option[Int]] = suppliers ++= Seq(
        (101, "Acme, Inc.", "99 Market Street", "Groundsville", "CA", "95199"),
        (49, "Superior Coffee", "1 Party Place", "Mendocino", "CA", "95460"),
        (150, "The High Ground", "100 Coffee Lane", "Meadows", "CA", "93966")
      )

      val insertCoffeesAction: DBIO[Option[Int]] = coffees ++= Seq(
        ("Colombian", 101, 7.99, 0, 0),
        ("French_Roast", 49, 8.99, 0, 0),
        ("Espresso", 150, 9.99, 0, 0),
        ("Colombian_Decaf", 101, 8.99, 0, 0),
        ("French_Roast_Decaf", 49, 9.99, 0, 0)
      )

      run(createSchemaAction >> insertSuppliersAction >> insertCoffeesAction)
      println("Setup complete...")
    }

    // Print the rows which contain the coffee name and the supplier name
    // Join the tables using the relationship defined in the Coffees table

    // select x2."COF_NAME", x3."SUP_NAME" from "COFFEES" x2, "SUPPLIERS" x3 where (x2."PRICE" > 9.0) and (x2."SUP_ID" = x3."SUP_ID")
    val simpleMonadicJoin = for {
      c <- coffees
      s <- suppliers if c.price > 9.0 && c.supID === s.id
    } yield (c.name, s.name)
    printResult("simpleMonadicJoin")(simpleMonadicJoin)

    val simpleMonadicJoin2 = for {
      c <- coffees if c.price > 9.0
      s <- suppliers if c.supID === s.id
    } yield (c.name, s.name)
    printResult("simpleMonadicJoin2")(simpleMonadicJoin2)

    // select x2."COF_NAME", x3."SUP_NAME" from "COFFEES" x2, "SUPPLIERS" x3 where (x2."PRICE" > 9.0) and (x3."SUP_ID" = x2."SUP_ID")
    val simpleMonadicJoinUsingReifiedFk: Query[(Rep[String], Rep[String]), (String, String), Seq] = for {
      c <- coffees if c.price > 9.0
      s <- c.supplier
    } yield (c.name, s.name)
    printResult("simpleMonadicJoinUsingReifiedFk")(simpleMonadicJoinUsingReifiedFk)

    // select x2."COF_NAME", x3."SUP_NAME" from "COFFEES" x2, "SUPPLIERS" x3 where (x2."PRICE" > 9.0) and (x2."SUP_ID" = x3."SUP_ID")
    val simpleApplicativeJoin = coffees
      .join(suppliers)
      .on { case (coffee, supplier) => coffee.supID === supplier.id } // .on(_.supID === _.id)
      .filter { case (coffee, _) => coffee.price > 9.0 } // .filter(_._1.price> 9.0 )
      .map { case (coffee, supplier) => (coffee.name, supplier.name) }
    printResult("simpleApplicativeJoin")(simpleApplicativeJoin)

    val clientSideJoin = db.run(coffees.result).flatMap { coffeeRows =>
      val filteredCoffeeRows = coffeeRows.filter {
        case (_, _, price, _, _) => price > 9.0
      }
      Future.traverse(filteredCoffeeRows) { case (name, supID, _, _, _) =>
        db.run(suppliers.filter(_.id === supID).result.headOption.map {
          case Some((_, supplierName, _, _, _, _)) => (name, supplierName)
        })
      }
    }

    // select c.COF_NAME, s.SUP_NAME from coffees c, suppliers s where c.PRICE > ? and s.SUP_ID = c.SUP_ID
    val plainSqlJoin = {
      def coffeeNameAndSupplierNameGtPrice(price: Double) =
        sql"""select c.COF_NAME, s.SUP_NAME from coffees c, suppliers s where c.PRICE > $price and s.SUP_ID = c.SUP_ID""".as[(String, String)]

      coffeeNameAndSupplierNameGtPrice(9.0)

      // We can use tsql interpolator instead of sql interpolator to match types
      // between Scala code and database. We'd need to annotate the caller object
      // with @StaticDatabaseConfig to give the compiler access to database.
      // This might make things hard to unit test.
      // Here's an example for demonstration purposes:
      // def coffeeNameAndSupplierNameGtPrice(price: Double) =
      //   tsql"""select c.COF_NAME, s.SUP_NAME from coffees c, suppliers s where c.PRICE > $price and s.SUP_ID = c.SUP_ID"""
    }
  } finally db.close

  private def await(future: Future[_]) = Await.result(future, 1 second)

  private def run(action: DBIO[_]) = await(db.run(action))

  private def execute(query: Query[(Rep[String], Rep[String]), (String, String), Seq]) = run(query.result)

  private def sql(query: Query[(Rep[String], Rep[String]), (String, String), Seq]) = query.result.statements

  private def printResult(name: String)(query: Query[(Rep[String], Rep[String]), (String, String), Seq]): Unit =
    println(s"Generated SQL for $name query:\n${sql(query)}\nResult: ${execute(query)}")

  private def printResult2(name: String)(action: SqlStreamingAction[Vector[(String, String)], (String, String), Effect]): Unit =
    println(s"Generated SQL for $name query:\n${action.statements}\nResult: ${await(db.run(action))}")
}
