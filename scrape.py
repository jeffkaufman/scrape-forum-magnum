#!/usr/bin/env python3

import os
import sys
import json
import glob
import requests
import datetime

LIMIT=100

queries = {
    "posts": """
{
  posts(input: {terms: {limit: %s, after: "%s", before: "%s"}}){
    results {
      _id
      title
      postedAt
      htmlBody
      user {
        username
      }
    }
  }
}
""",
    "comments": """
{
  comments(input: {terms: {limit: %s, after: "%s", before: "%s"}}){
    results {
      _id
      postedAt
      postId
      htmlBody
      parentCommentId
      user {
        username
      }
    }
  }
}
""",
    "post_comments": """
{
  comments(input: {terms: {view: "postCommentsOld", postId: "%s"}}){
    results {
      _id
      postedAt
      postId
      htmlBody
      parentCommentId
      user {
        username
      }
    }
  }
}
""",
}

endpoints = {
    "ea": " https://forum-bots.effectivealtruism.org/graphql?",
    "lw": "https://www.lesswrong.com/graphql?",
}

def query_one(server, query_name, after, before):
    populated_query = queries[query_name] % (
        LIMIT, after.isoformat(), before.isoformat())
    r = requests.post(
        endpoints[server],
        json={"query": populated_query},
        headers={"User-Agent": "jefftk"},
    )
    r.raise_for_status()
    return r.json()

START=datetime.datetime(1900,1,1)
END=datetime.datetime(2100,1,1)

def fetch_all(server, query_name, after=START, before=END, depth=0):
    print("  " * depth, after, before)
    r = query_one(server, query_name, after, before)

    results = r["data"][query_name]["results"]
    if len(results) < LIMIT:
        save_results(server, query_name, after, before, results)
        return

    del results
    del r

    middle = datetime.datetime.fromtimestamp(
        (after.timestamp() + before.timestamp()) / 2)
    fetch_all(server, query_name, after, middle, depth+1)
    fetch_all(server, query_name, middle, before, depth+1)

def fname(server, query_name, after, before):
    return "%s/%s/%s-%s.json" % (
        server, query_name, after.isoformat(), before.isoformat())

def save_results(server, query_name, after, before, results):
    with open(fname(server, query_name, after, before), "w") as outf:
        json.dump(results, outf)

def fetch_single_post_comments(server, query_name, post_id):
    populated_query = queries[query_name] % (
        post_id)
    r = requests.post(
        endpoints[server],
        json={"query": populated_query},
        headers={"User-Agent": "jefftk"},
    )
    r.raise_for_status()
    return r.json()["data"]["comments"]["results"]

def fetch_post_comments(server, query_name):
    post_ids = {}
    for json_fname in glob.glob("%s/posts/*.json" % server):
        with open(json_fname) as inf:
            for post in json.load(inf):
                post_ids[post["_id"]] = post["title"]
    for post_id, title in sorted(post_ids.items()):
        post_comments_fname = "%s/%s/%s.json" % (
            server, query_name, post_id)
        if os.path.exists(post_comments_fname):
            continue
        print("%s (%s)..." % (title, post_id))
        r = fetch_single_post_comments(server, query_name, post_id)
        with open(post_comments_fname, "w") as outf:
            json.dump(r, outf)

if __name__ == "__main__":
    server, query_name = sys.argv[1:]

    try:
        os.mkdir(server)
    except FileExistsError:
        pass

    try:
        os.mkdir(os.path.join(server, query_name))
    except FileExistsError:
        pass

    if query_name in ["posts", "comments"]:
        fetch_all(server, query_name)
    elif query_name == "post_comments":
        fetch_post_comments(server, query_name)
    else:
        raise Exception("Unknown query %s" % query_name)
