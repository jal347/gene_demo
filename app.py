from opensearchpy import OpenSearch, exceptions
from flask import Flask, request, Response, url_for, render_template, redirect
import json
from utils import set_key

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
client = OpenSearch("search-gene-y5n3e4j4sgnn7rigtjlod6hboq.us-east-2.es.amazonaws.com:80")


@app.route('/gene/')
@app.route('/gene/<gene_id>')
def annotation(gene_id=None):

    # checks if id exists if not return error
    if gene_id is None:
        doc = {
            "code": 400,
            "success": False,
            "error": "Bad Request",
            "missing": "id"
        }
        return doc, 400

    fields = None
    args = request.args
    if "fields" in args:
        fields = args["fields"].split(",")

    # error handing exception to see if id is valid
    try:
        response = client.get(index="mygene", id=gene_id, _source_includes=fields)
    except exceptions.NotFoundError:
        doc = {
            "code": 404,
            "success": False,
            "error": "Gene ID '" + gene_id + "' not found"
        }
        return doc, 404

    doc = {
        "_id": response["_id"],
        "_version": response["_version"]
    }
    doc.update(response["_source"])

    return doc



# this route will only work if it has a q or aggs field
@app.route("/query")
def query():

    args = request.args
    size = 10
    start = 0
    fields = None
    search_param = {}
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
        search_param_2 = {
            "aggs": {
                "facets": {
                    "terms": {"field": field}
                }
            }
        }
        search_param.update(search_param_2)



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


    # make the response look like mygene.info
    doc = {
        "took": response["took"],
        "total": response["hits"]["total"]["value"],
        "max_score": response["hits"]["max_score"]
    }

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
            return Response(json.dumps(doc, indent=4), headers={"Content-Type": "application/json"})

    return doc


