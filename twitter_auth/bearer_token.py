import twitter_auth.twitter_secrets as twitter_secrets


def get_bearer_oauth_header(r):

    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {twitter_secrets.bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"

    return r
