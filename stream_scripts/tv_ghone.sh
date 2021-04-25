#!/usr/bin/env bash
#
################################################
### GhONE TV segments streaming script ###
################################################
#
# Stream GhONE TV while recording chunks of 15 seconds
exec /usr/bin/ffmpeg -i "rtmp://107.6.164.246/app/ghone" -vn -acodec aac -c copy -ar 48000 -b:a 256k -f stream_segment -segment_list /home/blazoninnovation/testbed/test_streams/ghana/tv/ghone_stream_watcher/ghone.csv -segment_time 15 -segment_list_flags +live -reset_timestamps 1 -strftime 1 /home/blazoninnovation/testbed/test_streams/ghana/tv/ghone_stream_watcher/ghone%Y-%m-%d_%H-%M-%S.aac