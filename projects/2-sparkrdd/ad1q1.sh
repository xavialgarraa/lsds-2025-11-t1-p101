#!/bin/bash

input_file="wikipedia2.json"  
output_dir="output_files"  

mkdir -p "$output_dir"

while IFS= read -r line; do
    article_id=$(echo "$line" | jq -r '.identifier')

    if [ "$article_id" != "null" ]; then
        echo "$line" > "$output_dir/$article_id.json"
        echo "Article $article_id written to $output_dir/$article_id.json"
    fi
done < "$input_file"
