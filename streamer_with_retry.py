from streamer.RateLimitEncounteredError import RateLimitEncounteredError
from streamer.StreamReceiveError import StreamReceiveError
import streamer.live_tweet_streamer as live_tweet_streamer
import time
import logging

logger = logging.getLogger(__name__)

_should_continue = False
_streamer = None


def start(on_data):
    global _should_continue
    global _streamer
    _should_continue = True

    while _should_continue:
        try:
            # TODO: instead of linear back off implement exponential back off with some upper limit
            back_off_time_in_seconds = 5
            logger.info("Going to create new streamer")
            _streamer = live_tweet_streamer.TStreamer(on_data)
            _streamer.start()
        except RateLimitEncounteredError as rle:
            back_off_time_in_seconds = 300
            logger.error("Rate Limit Error from TStreamer : " + str(rle))
            _streamer.stop()
        except StreamReceiveError as sre:
            back_off_time_in_seconds = 20
            logger.error("Error from TStreamer : " + str(sre))
            _streamer.stop()
        except Exception as e:
            logger.error("Error from TStreamer : " + str(e))
            _streamer.stop()

        # TODO: Twitter document speaks about different back off times while
        # retrying for network error, http error and http 420 error
        if _should_continue:
            logger.info("Going to sleep before retrying with TStreamer")
            time.sleep(back_off_time_in_seconds)

    logger.info("Came out of main loop")


def stop():
    global _should_continue
    global _streamer
    _should_continue = False
    if _streamer is not None:
        _streamer.stop()
