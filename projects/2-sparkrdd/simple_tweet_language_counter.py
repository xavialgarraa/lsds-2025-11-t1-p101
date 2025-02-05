import json

def parse_tweet(line):
    tweet = json.loads(line)
    return tweet.get("lang", "unknown") 
    

language_count = {}

with open("data/Eurovision3.json", "r", encoding="utf-8") as file_in:
    for line in file_in:
        line = line.strip()
        if not line:
            continue  
        
        lang = parse_tweet(line)
        if lang:
            if lang not in language_count:
                language_count[lang] = 0  
            language_count[lang] += 1  

print(language_count)

