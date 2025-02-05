import json
from dataclasses import dataclass
from typing import Optional

@dataclass
class Tweet:
    tweet_id: int
    text: str
    user_id: int
    user_name: str
    language: str
    timestamp_ms: int
    retweeted_id: Optional[int]
    retweeted_user_id: Optional[int]

def parse_tweet(tweet: str) -> Tweet:
    data = json.loads(tweet)
    return Tweet(
        tweet_id=data["id"],
        text=data["text"],
        user_id=data["user"]["id"],
        user_name=data["user"]["name"],
        language=data["lang"],
        timestamp_ms=int(data["timestamp_ms"]),
        retweeted_id=data["retweeted_status"]["id"] if "retweeted_status" in data else None,
        retweeted_user_id=data["retweeted_status"]["user"]["id"] if "retweeted_status" in data else None,
    )

if __name__ == "__main__":
    with open("Eurovision3.json", "r", encoding="utf-8") as f:
        first_line = f.readline().strip()
        tweet = parse_tweet(first_line)
        print(tweet)
