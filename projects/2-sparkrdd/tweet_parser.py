import json
from dataclasses import dataclass
from typing import Optional

@dataclass
class Tweet:
    tweet_id: Optional[int]
    text: Optional[str]
    user_id: Optional[int]
    user_name: Optional[str]
    language: Optional[str]
    timestamp_ms: Optional[int]
    retweeted_id: Optional[int]
    retweeted_user_id: Optional[int]
    retweeted_user_name: Optional[str]

def parse_tweet(tweet: str) -> Tweet:
    try:
        data = json.loads(tweet)
        return Tweet(
            tweet_id=data.get("id"),
            text=data.get("text"),
            user_id=data["user"]["id"] if "user" in data else None,
            user_name=data["user"]["name"] if "user" in data else None,
            language=data.get("lang"),
            timestamp_ms=int(data["timestamp_ms"]) if "timestamp_ms" in data else None,
            retweeted_id=data["retweeted_status"]["id"] if "retweeted_status" in data else None,
            retweeted_user_id=data["retweeted_status"]["user"]["id"] if "retweeted_status" in data else None,
            retweeted_user_name=data["retweeted_status"]["user"]["name"] if "retweeted_status" in data and "user" in data["retweeted_status"] else None,  
        )
    except json.JSONDecodeError:
        return None

if __name__ == "__main__":
    with open("data/Eurovision3.json", "r", encoding="utf-8") as f:
        first_line = f.readline().strip()
        tweet = parse_tweet(first_line)
        print(tweet)
        