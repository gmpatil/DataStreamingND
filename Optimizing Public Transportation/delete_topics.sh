# !/bin/bash
# Topics preferred to be created ahead.
declare -a topics=( 
"com.cta.station.datat001.stations" 
"com.cta.station.datat001.stations-faust_tst.transform_stations-stations-stream-tst-Station.station_id-repartition"
"com.cta.station.datat002.stations"
"com.cta.station.datat003.stations"
"com.cta.station.datat003x.stations"
"om.cta.station.datat001.stations"
"om.cta.station.datat002.stations"
"stations-stream-tst-__assignor-__leader"
"stations-stream-tst-tranformed_station_stopst001-changelog"
)

for t in "${topics[@]}"; do
    kafka-topics --delete --zookeeper localhost:2181 --topic $t
done


