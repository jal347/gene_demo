from opensearchpy import OpenSearch
from flask import Flask, request, url_for, render_template, redirect
import requests
import features
import logging
import json
from utils import set_key

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
client = OpenSearch("search-gene-y5n3e4j4sgnn7rigtjlod6hboq.us-east-2.es.amazonaws.com:80")


@app.route('/gene/<gene_id>')
def annotation(gene_id):

    # search_param = {
    #     "query": {
    #         "terms": {
    #             "_id": [gene_id]
    #         }
    #     }
    # }
    #
    # response = client.search(index="mygene", body=search_param)
    #
    # if len(response["hits"]["hits"][0]):
    #     return response
    # else:
    #     return 404

    response = requests.get("https://search-gene-y5n3e4j4sgnn7rigtjlod6hboq.us-east-2.es.amazonaws.com/mygene/_doc/" +
                            gene_id)
    response = response.json()

    #if we can access a document with the id
    if response["found"]:
        doc = {}
        set_key(doc, "_id", response["_id"])
        set_key(doc, "_version", response["_version"])
        doc.update(response["_source"])
    else:
        doc = {
            "code": 404,
            "success": bool(False),
            "error": "Gene ID " + "'" + gene_id + "' not found"
        }
    return doc


# this route will only work if it has a q or aggs field
@app.route("/query")
def query():

    args = request.args
    size = 10
    start = 0

    # q field
    if "q" in args:
        q = args["q"]

        # make a query with the q field
        search_param = {
            "query": {
                "query_string": {
                    "query": q
                }
            }
        }

    # if aggs field
    if "aggs" in args:
        field = args["aggs"]
        search_param = {
            "aggs": {
                "facets": {
                    "terms": {"field": field}
                }
            }
        }


    # checks for size parameter (size is defaulted at 10)
    if "size" in args:
        size = int(args["size"])

    # checks if from is there
    if "from" in args:
        start = int(args["from"])

    if "fields" in args:
        # get the fields in a list
        fields = args["fields"].split(",")
        response = client.search(index="mygene", body=search_param, size=size, _source_includes=fields, from_=start)
    else:
        response = client.search(index="mygene", body=search_param, size=size, from_=start)


    # make the response look like mygene.info
    doc = {}
    set_key(doc, "took", response["took"])
    set_key(doc, "total", response["hits"]["total"]["value"])
    set_key(doc, "max_score", response["hits"]["max_score"])
    if "aggs" in args:
        aggregation = {
            "facets": {
                field: {
                    "_type": "terms",
                    "terms": response["aggregations"]["facets"]['buckets']
                }
            }
        }
        doc.update(aggregation)

    set_key(doc, "hits", [])
    for hit in response["hits"]["hits"]:
        info = {
            "_id": hit["_id"],
            "_score": hit["_score"]
        }
        if hit.get("_source") is not None:
            info.update(hit["_source"])
        set_key(doc, "hits", info)


    # check if pretty print is true defaulted false
    if "pretty" in args:
        if args["pretty"].lower() == "true":
            app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True

    return doc


