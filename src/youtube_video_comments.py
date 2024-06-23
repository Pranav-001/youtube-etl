# -*- coding: utf-8 -*-
import os

import googleapiclient.discovery
import pandas as pd

DEVELOPER_KEY = "SECRET VALUE"
VIDEO_ID = "q8q3OFFfY6c"


def process_comments(response_items):
    comments = []
    for comment in response_items:
        author = comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
        comment_text = comment["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
        publish_time = comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
        comment_info = {
            "author": author,
            "comment": comment_text,
            "published_at": publish_time,
        }
        comments.append(comment_info)
    print(f"Finished processing {len(comments)} comments.")
    return comments


def run_youtube_etl():
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=DEVELOPER_KEY
    )

    request = youtube.commentThreads().list(part="snippet, replies", videoId=VIDEO_ID)
    response = request.execute()
    comments_list = []
    while response.get("nextPageToken", None):
        request = youtube.commentThreads().list(
            part="id,replies,snippet",
            videoId=VIDEO_ID,
            pageToken=response["nextPageToken"],
        )
        response = request.execute()
        comments_list.extend(process_comments(response["items"]))

    df = pd.DataFrame(comments_list)
    df.to_csv("s3://play-bucket-dev/refined_tweets.csv")
