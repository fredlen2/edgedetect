#!/usr/bin/env bash
#
################################################
### Peace FM example segments streaming script ###
################################################
#
# Stream Peace FM while recording chunks of 15 seconds
#
exec /usr/bin/ffmpeg -i "http://peacefm.atunwadigital.streamguys1.com/peacefm" -r 15 -acodec mp3 -c copy -f stream_segment -segment_list /home/blazoninnovation/testbed/test_streams/ghana/radio/peace_fm_stream_watcher/peacefm.csv -segment_time 10 -segment_list_flags +live -reset_timestamps 1 -b:a 64k -strftime 1 /home/blazoninnovation/testbed/test_streams/ghana/radio/peace_fm_stream_watcher/peacefm%Y-%m-%d_%H-%M-%S.mp3