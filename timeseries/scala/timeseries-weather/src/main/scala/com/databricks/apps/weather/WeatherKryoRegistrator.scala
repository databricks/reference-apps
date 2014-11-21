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

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class WeatherKryoRegistrator extends KryoRegistrator {
  import Weather._
  import WeatherEvent._

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[RawWeatherData])
    kryo.register(classOf[Day])
    kryo.register(classOf[DailyPrecipitation])
    kryo.register(classOf[AnnualPrecipitation])
    kryo.register(classOf[TopKPrecipitation])
    kryo.register(classOf[DailyTemperature])
    kryo.register(classOf[MonthlyTemperature])
    kryo.register(classOf[GetWeatherStation])
    kryo.register(classOf[GetCurrentWeather])
    kryo.register(classOf[GetPrecipitation])
    kryo.register(classOf[GetTopKPrecipitation])
    kryo.register(classOf[GetDailyTemperature])
    kryo.register(classOf[GetMonthlyHiLowTemperature])
    kryo.register(classOf[GetMonthlyTemperature])
    kryo.register(classOf[NoDataAvailable])
    kryo.register(classOf[WeatherStation])
  }
}