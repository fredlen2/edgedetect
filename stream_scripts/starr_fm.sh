#!/usr/bin/env bash
#################################################
### Star FM example segments streaming script ###
#################################################
#
# Stream Star FM while recording chunks of 15 seconds
exec /usr/bin/ffmpeg -i "http://ample-zeno-01.radiojar.com/ncwsgzh7hewtv?rj-token=AAABWIZzAcKd209p35VcXuns6Gst-Yo4SnTVxm23cC9KWxhO87alnA" -r 15 -acodec mp3 -c copy -f stream_segment -segment_list /home/blazoninnovation/testbed/test_streams/ghana/radio/starr_fm_stream_watcher/starrfm.csv -segment_time 10 -segment_list_flags +live -reset_timestamps 1 -strftime 1 /home/blazoninnovation/testbed/test_streams/ghana/radio/starr_fm_stream_watcher/starrfm%Y-%m-%d_%H-%M-%S.mp3