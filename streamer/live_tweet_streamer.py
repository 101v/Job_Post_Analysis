import requests
from streamer.RateLimitEncounteredError import RateLimitEncounteredError
from streamer.StreamReceiveError import StreamReceiveError
import twitter_urls
import twitter_auth.bearer_token as bearer_token
import logging

logger = logging.getLogger(__name__)


class TStreamer:

    def __init__(self, on_data) -> None:
        self._should_continue = False
        self.on_data = on_data
        self.tweet_counter = 0
        self.ongoing_response = None

    def start(self):
        self._should_continue = True

        # TODO: should we use session instead to close underlying connection?
        #  As of now taken care using with keyword
        with requests.get(
            twitter_urls.stream_endpoint,
            auth=bearer_token.get_bearer_oauth_header,
            stream=True,
            timeout=40
        ) as response:

            self.ongoing_response = response
            if response.status_code != 200:
                if response.status_code == 429:
                    raise RateLimitEncounteredError(
                        "Too Many Requests. Rate limit breached. (HTTP {}): {}"
                        .format(
                            response.status_code, response.text)
                        )
                else:
                    raise StreamReceiveError(
                        "HTTP Error (HTTP {}): {}".format(
                            response.status_code, response.text)
                        )

            logger.info("Stream connected")
            for response_line in response.iter_lines():
                if response_line:
                    self.tweet_counter += 1
                    response_line_str = response_line.decode("utf-8")
                    self.on_data(response_line_str)
                else:
                    logger.info("Keep alive received")

                if not self._should_continue:
                    break

        logger.info("Connection broken")

    def stop(self):
        self._should_continue = False
        try:
            self.ongoing_response.close()
        except Exception as e:
            logger.error(
                "Error while closing the connection on stop. Message : {0}"
                .format(e)
            )
