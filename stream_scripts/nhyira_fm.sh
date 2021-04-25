#!/usr/bin/env bash
#
################################################
### NHYIRA FM segments streaming script ###
################################################
#
# Stream Nhyira FM while recording chunks of 10 seconds
exec /usr/bin/ffmpeg -i "http://mmg.streamguys1.com/NhyiraFM-mp3?key=93fe20a16f78c70991fa726b9ca9c19c49f7329a3ad6144500e8fe7f3b8dadbafa6e84e66e84f9d149b17181fcf7194f" -r 15 -acodec mp3 -c copy -f stream_segment -segment_list /home/blazoninnovation/testbed/test_streams/ghana/radio/nhyira_fm_stream_watcher/nhyirafm.csv -segment_time 10 -segment_list_flags +live -reset_timestamps 1 -b:a 64k -strftime 1 /home/blazoninnovation/testbed/test_streams/ghana/radio/nhyira_fm_stream_watcher/nhyirafm%Y-%m-%d_%H-%M-%S.mp3