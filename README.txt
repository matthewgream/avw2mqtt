
Simple lightweight service to fetch METAR/TAFs from AviationWeather.gov and push them to MQTT, in both readable and raw formats. Implemented in 'C' for low running footprint.

- fetch METAR and/or TAF for multiple ICAO codes at configured periodicity
- option to learn and adapt fetch times and periods to match METAR/TAF publishing
- option to blend in blend in airport metadata (name, lat/lon, elevation, ...)

- publish to specified MQTT broker (with authentication, if configured) and topic in JSON
- publish timestamp, airport data, and METAR and TAF, each in both raw and text formats
- publish only when updated (by METAR observation date, and TAF issued date), or always publsh
- publish combined (METAR and TAF in same message) or split (separate METAR and TAF topics) to MQTT

- run on command line with debugging output, or run as systemd service (service file included)

requires: mosquitto lib, cJSON lib, XML lib, Curl lib

