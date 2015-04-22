/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.apps.weather

import akka.actor.ActorRef
import org.apache.spark.util.StatCounter
import org.joda.time.DateTime

object Weather {

  /** Base marker trait. */
  @SerialVersionUID(1L)
  sealed trait WeatherModel extends Serializable

  /**
   * @param id Composite of Air Force Datsav3 station number and NCDC WBAN number
   * @param name Name of reporting station
   * @param countryCode 2 letter ISO Country ID // TODO restrict
   * @param callSign International station call sign
   * @param lat Latitude in decimal degrees
   * @param long Longitude in decimal degrees
   * @param elevation Elevation in meters
   */
  case class WeatherStation(
    id: String,
    name: String,
    countryCode: String,
    callSign: String,
    lat: Double,
    long: Double,
    elevation: Double) extends WeatherModel

  /**
   * @param wsid Composite of Air Force Datsav3 station number and NCDC WBAN number
   * @param year Year collected
   * @param month Month collected
   * @param day Day collected
   * @param hour Hour collected
   * @param temperature Air temperature (degrees Celsius)
   * @param dewpoint Dew point temperature (degrees Celsius)
   * @param pressure Sea level pressure (hectopascals)
   * @param windDirection Wind direction in degrees. 0-359
   * @param windSpeed Wind speed (meters per second)
   * @param skyCondition Total cloud cover (coded, see format documentation)
   * @param skyConditionText Non-coded sky conditions
   * @param oneHourPrecip One-hour accumulated liquid precipitation (millimeters)
   * @param sixHourPrecip Six-hour accumulated liquid precipitation (millimeters)
   */
  case class RawWeatherData(
    wsid: String,
    year: Int,
    month: Int,
    day: Int,
    hour: Int,
    temperature: Double,
    dewpoint: Double,
    pressure: Double,
    windDirection: Int,
    windSpeed: Double,
    skyCondition: Int,
    skyConditionText: String,
    oneHourPrecip: Double,
    sixHourPrecip: Double) extends WeatherModel

  object RawWeatherData {
    /** Tech debt */
    def apply(array: Array[String]): RawWeatherData = {
      RawWeatherData(
        wsid = array(0),
        year = array(1).toInt,
        month = array(2).toInt,
        day = array(3).toInt,
        hour = array(4).toInt,
        temperature = array(5).toDouble,
        dewpoint = array(6).toDouble,
        pressure = array(7).toDouble,
        windDirection = array(8).toInt,
        windSpeed = array(9).toDouble,
        skyCondition = array(10).toInt,
        skyConditionText = array(11),
        oneHourPrecip = array(11).toDouble,
        sixHourPrecip = Option(array(12).toDouble).getOrElse(0))
    }
  }

  trait WeatherAggregate extends WeatherModel with Serializable {
    def wsid: String
    def year: Int
  }

  case class Day(wsid: String, year: Int, month: Int, day: Int) extends WeatherAggregate

  object Day {
    def apply(d: RawWeatherData): Day =
      Day(d.wsid, d.year, d.month, d.day)

    def apply(wsid: String, utcTimestamp: DateTime): Day =
      Day(wsid, utcTimestamp.getYear, utcTimestamp.getMonthOfYear, utcTimestamp.getDayOfMonth)

    /** Tech debt */
    def apply(line: String): Day = {
      val array = line.split(",")
      Day(wsid = array(0), year = array(1).toInt, month = array(2).toInt, day = array(3).toInt)
    }
  }

  case class NoDataAvailable(wsid: String, year: Int, query: Class[_ <: WeatherAggregate]) extends WeatherAggregate

  /* Precipitation */
  trait Precipitation extends WeatherAggregate

  case class DailyPrecipitation(wsid: String,
                                year: Int,
                                month: Int,
                                day: Int,
                                precipitation: Double) extends Precipitation

  case class AnnualPrecipitation(wsid: String,
                                 year: Int,
                                 total: Double) extends Precipitation
  object AnnualPrecipitation {
    def apply(aggregate: Seq[Double], wsid: String, year: Int): AnnualPrecipitation =
      AnnualPrecipitation(wsid, year, aggregate.sum)
  }

  case class TopKPrecipitation(wsid: String,
                               year: Int,
                               top: Seq[Double]) extends WeatherAggregate

  /* Temperature */
  trait Temperature extends WeatherAggregate

  case class DailyTemperature(wsid: String,
                              year: Int,
                              month: Int,
                              day: Int,
                              high: Double,
                              low: Double,
                              mean: Double,
                              variance: Double,
                              stdev: Double) extends Temperature

  object DailyTemperature {
    def apply(key:Day, stats: StatCounter): DailyTemperature =
      DailyTemperature(
        key.wsid,
        key.year,
        key.month,
        key.day,
        high = stats.max,
        low = stats.min,
        mean = stats.mean,
        variance = stats.variance,
        stdev = stats.stdev)
  }

  case class MonthlyTemperature(wsid: String,
                         year: Int,
                         month: Int,
                         high: Double,
                         low: Double) extends Temperature

}

object WeatherEvent {
  import Weather._

  /** Base marker trait. */
  @SerialVersionUID(1L)
  sealed trait AppEvent extends Serializable

  sealed trait LifeCycleEvent extends AppEvent
  case object OutputStreamInitialized extends LifeCycleEvent
  case class NodeInitialized(root: ActorRef) extends LifeCycleEvent
  case object DataFeedStarted extends LifeCycleEvent
  case object Shutdown extends LifeCycleEvent
  case object TaskCompleted extends LifeCycleEvent

  @SerialVersionUID(1L)
  sealed trait WeatherRequest extends Serializable

  trait WeatherStationRequest extends WeatherRequest
  case class GetWeatherStation(sid: String) extends WeatherStationRequest
  case class GetCurrentWeather(wsid: String, timestamp: Option[DateTime]= None) extends WeatherStationRequest

  trait PrecipitationRequest extends WeatherRequest
  case class GetPrecipitation(wsid: String, year: Int) extends PrecipitationRequest
  case class GetTopKPrecipitation(wsid: String, year: Int, k: Int) extends PrecipitationRequest

  trait TemperatureRequest extends WeatherRequest
  case class GetDailyTemperature(day: Day) extends TemperatureRequest
  case class GetMonthlyHiLowTemperature(wsid: String, year: Int, month: Int) extends TemperatureRequest
  case class GetMonthlyTemperature(wsid: String, year: Int, month: Int) extends TemperatureRequest
}