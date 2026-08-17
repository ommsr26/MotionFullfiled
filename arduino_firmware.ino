/*
  ===========================================================================
  Solid Waste Motion Detection - Arduino Node Firmware (DS3231 RTC + NEO-6M)
  ===========================================================================
  
  Streams hardware timestamp and GPS coordinates over USB Serial to the Python
  motion detection application.

  Serial Protocol Output:
    DATA,YYYY-MM-DD HH:MM:SS,Latitude,Longitude,GPS_Valid_Flag
    Example: DATA,2026-08-17 17:25:30,12.971598,77.594562,1

  Baud Rate: 115200
  
  Hardware Wiring:
  ----------------
  1. DS3231 RTC Module (I2C):
     - VCC  -> Arduino 5V
     - GND  -> Arduino GND
     - SDA  -> Arduino A4 (or dedicated SDA pin near AREF)
     - SCL  -> Arduino A5 (or dedicated SCL pin near AREF)

  2. NEO-6M GPS Module (UART via SoftwareSerial):
     - VCC  -> Arduino 5V (or 3.3V depending on module)
     - GND  -> Arduino GND
     - TX   -> Arduino Digital Pin 4 (SoftwareSerial RX)
     - RX   -> Arduino Digital Pin 3 (SoftwareSerial TX, via 1k/2k voltage divider if 3.3V logic)

  Required Arduino Libraries:
  ---------------------------
  - RTClib by Adafruit (Install via Arduino Library Manager)
  - TinyGPS++ by Mikal Hart (Install via Arduino Library Manager, optional)
*/

#include <Wire.h>
#include "RTClib.h"
#include <SoftwareSerial.h>

// GPS Serial configuration (RX=4, TX=3)
SoftwareSerial gpsSerial(4, 3);

RTC_DS3231 rtc;
bool rtc_available = false;

// Simple GPS NMEA coordinate storage
float gps_lat = 0.0;
float gps_lon = 0.0;
bool gps_valid = false;

void setup() {
  Serial.begin(115200);
  gpsSerial.begin(9600);
  Wire.begin();

  delay(1000);
  Serial.println(F("[ARDUINO] Initializing sensors..."));

  // 1. Initialize DS3231 RTC
  if (!rtc.begin()) {
    Serial.println(F("[RTC ERROR] Could not find DS3231 RTC module!"));
    Serial.println(F("[RTC HINT] Verify I2C connections: SDA->A4, SCL->A5, VCC 5V, GND"));
    rtc_available = false;
  } else {
    rtc_available = true;
    Serial.println(F("[RTC OK] DS3231 RTC found and initialized."));

    // If RTC lost power (e.g. coin cell was removed), set time to sketch compilation time
    if (rtc.lostPower()) {
      Serial.println(F("[RTC WARN] RTC lost power! Setting time to compilation timestamp."));
      rtc.adjust(DateTime(F(__DATE__), F(__TIME__)));
    }
  }

  Serial.println(F("[ARDUINO] Setup complete. Starting data stream..."));
}

void loop() {
  // Read incoming bytes from GPS module if available
  while (gpsSerial.available() > 0) {
    char c = gpsSerial.read();
    // Simple NMEA parsing can be handled here or via TinyGPS++
  }

  // Format RTC time string
  char timeBuffer[25];
  if (rtc_available || rtc.begin()) {
    rtc_available = true;
    DateTime now = rtc.now();

    // Check if RTC returned valid year (>= 2020)
    if (now.year() >= 2020 && now.year() <= 2099) {
      sprintf(timeBuffer, "%04d-%02d-%02d %02d:%02d:%02d",
              now.year(), now.month(), now.day(),
              now.hour(), now.minute(), now.second());
    } else {
      // Re-adjust if time corrupted
      rtc.adjust(DateTime(F(__DATE__), F(__TIME__)));
      DateTime adjusted = rtc.now();
      sprintf(timeBuffer, "%04d-%02d-%02d %02d:%02d:%02d",
              adjusted.year(), adjusted.month(), adjusted.day(),
              adjusted.hour(), adjusted.minute(), adjusted.second());
    }
  } else {
    strcpy(timeBuffer, "Waiting for RTC...");
  }

  // Transmit standard packet over Serial to Python
  Serial.print(F("DATA,"));
  Serial.print(timeBuffer);
  Serial.print(F(","));
  Serial.print(gps_lat, 6);
  Serial.print(F(","));
  Serial.print(gps_lon, 6);
  Serial.print(F(","));
  Serial.println(gps_valid ? 1 : 0);

  delay(1000);
}
