#!/usr/bin/env bash
#
################################################
### CITI FM example segments streaming script ###
################################################
#
# Stream Citi FM while recording chunks of 15 seconds
exec /usr/bin/ffmpeg -i "http://ample-zeno-24.radiojar.com/6gty30d3v3duv?rj-ttl=5&rj-token=AAABaPJZzDAEqZbxRJlpkU7ww6uuawgeOy8LfHLHoH_xo1cIKCRVbA" -r 15 -acodec mp3 -c copy -f stream_segment -segment_list /home/blazoninnovation/testbed/test_streams/ghana/radio/citi_fm_stream_watcher/citifm.csv -segment_time 10 -segment_list_flags +live -reset_timestamps 1 -b:a 64k -strftime 1 /home/blazoninnovation/testbed/test_streams/ghana/radio/citi_fm_stream_watcher/citifm%Y-%m-%d_%H-%M-%S.mp3