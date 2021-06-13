package ca.mcit.bigdata.sprint2

import io.circe.{Decoder, HCursor}

case class SystemInformation( system_id : String, language : String, name : String, short_name : Option[String],
                              operator : Option[String], url : Option[String], purchase_url : Option[String],
                              start_date : Option[String], phone_number : Option[String], email : Option[String],
                              timezone : String, license_url : Option[String] )
object SystemInformation{
    implicit val decode : Decoder[SystemInformation] = (c: HCursor) => for {
      system_id <- c.downField("data").downField("system_id").as[String]
      language <- c.downField("data").downField("language").as[String]
      name <- c.downField("data").downField("name").as[String]
      short_name <- c.downField("data").downField("short_name").as[Option[String]]
      operator <- c.downField("data").downField("operator").as[Option[String]]
      url <- c.downField("data").downField("url").as[Some[String]]
      purchase_url <- c.downField("data").downField("purchase_url").as[Option[String]]
      start_date <- c.downField("data").downField("start_date").as[Option[String]]
      phone_number <- c.downField("data").downField("phone_number").as[Option[String]]
      email <- c.downField("data").downField("email").as[Option[String]]
      timezone <- c.downField("data").downField("timezone").as[String]
      license_url <- c.downField("data").downField("license_url").as[Option[String]]
    }
    yield {
      SystemInformation(system_id, language, name, short_name, operator, url, purchase_url, start_date, phone_number,
                        email, timezone, license_url)
    }

   def toCsv(input : SystemInformation) : String = {
      input.system_id + "," +
      input.language + "," +
      input.name + "," +
      input.short_name.getOrElse("") + "," +
      input.operator.getOrElse("") + "," +
      input.url.getOrElse("") + "," +
      input.purchase_url.getOrElse("") + "," +
      input.start_date.getOrElse("") + "," +
      input.phone_number.getOrElse("") + "," +
      input.email.getOrElse("") + "," +
      input.timezone + "," +
      input.license_url.getOrElse("")
  }
}