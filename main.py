# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import json
from opensearchpy import OpenSearch
import requests
import logging

logging.basicConfig(level=logging.DEBUG)

# host = "search-gene-y5n3e4j4sgnn7rigtjlod6hboq.us-east-2.es.amazonaws.com"
# port = 9200
# auth = ('admin', 'admin') # For testing only. Don't store credentials in code.
# ca_certs_path = '/full/path/to/root-ca.pem' # Provide a CA bundle if you use intermediate CAs with your root CA.

# Optional client certificates if you don't want to use HTTP basic authentication.
# client_cert_path = '/full/path/to/client.pem'
# client_key_path = '/full/path/to/client-key.pem'

# Create the client with SSL/TLS enabled, but hostname verification disabled.
# client = OpenSearch(
#     hosts = [{'host': host, 'port': port}],
#     http_compress = True, # enables gzip compression for request bodies
#     http_auth = auth,
#     # client_cert = client_cert_path,
#     # client_key = client_key_path,
#     use_ssl = True,
#     verify_certs = True,
#     ssl_assert_hostname = False,
#     ssl_show_warn = False,
#     ca_certs = ca_certs_path
# )
client = OpenSearch("search-gene-y5n3e4j4sgnn7rigtjlod6hboq.us-east-2.es.amazonaws.com:80")
"""
# gets a number of the first (x) documents from mygene.info API assume size > 0
def get_documents(documents):

    max_size = 1000
    end = 0
    while documents > 0:
        if documents >= max_size:
            response = requests.get("http://mygene.info/v3/query?size=" + str(max_size) + "&from=" + str(end) +
                                "&fields=name,symbol,refseq.rna")
            response = response.json()
            response = response["hits"]

            documents = documents - max_size
            end = end + max_size

            fp = open("documents.json", "r")
            data = json.load(fp)
            if data.get("hits") is None:
                data["hits"] = response
                fp.seek(0)
                fp.close()
            else:
                data["hits"] = data["hits"] + response
                fp.seek(0)
                fp.close()

            fp = open("documents.json", "w")
            json.dump(data, fp, indent=4)
        else:
            response = requests.get("http://mygene.info/v3/query?size=" + str(documents) + "&from=" + str(end) +
                                "&fields=name,symbol,refseq.rna")
            response = response.json()
            response = response["hits"]

            documents = 0

            fp = open("documents.json", "r")
            data = json.load(fp)
            if data.get("hits") is None:
                data["hits"] = response
                fp.seek(0)
                fp.close()
            else:
                data["hits"] = data["hits"] + response
                fp.seek(0)
                fp.close()

            fp = open("documents.json", "w")
            json.dump(data, fp, indent=4)

    fp = open("documents.json", "r")
    data = json.load(fp)
    print(len(data["hits"]))
"""


def get_cdk():

    cdks = ["cdk", "cdk2", "cdk3"]
    max_size = 1000
    for cdk in cdks:
        # query
        response = requests.get("http://mygene.info/v3/query?q=" + cdk + "&fetch_all=TRUE")
        response = response.json()
        # get the documents
        hits = response["hits"]
        # scroll id to continue
        scroll_id = response["_scroll_id"]

        fp = open("cdk.json", "r")
        data = json.load(fp)
        data["hits"] = data["hits"] + hits
        fp.seek(0)
        fp.close()
        fp = open("cdk.json", "w")
        json.dump(data, fp, indent=4)

        total = int(response["total"])
        total = total//max_size

        while total > 0:
            response = requests.get("http://mygene.info/v3/query?scroll_id=" + scroll_id)
            response = response.json()
            # get the documents
            hits = response["hits"]
            # scroll id to continue
            scroll_id = response["_scroll_id"]

            fp = open("cdk.json", "r")
            data = json.load(fp)
            data["hits"] = data["hits"] + hits
            fp.seek(0)
            fp.close()
            fp = open("cdk.json", "w")
            json.dump(data, fp, indent=4)

            total = total - 1
    fp = open("cdk.json", "r")
    data = json.load(fp)
    print(len(data["hits"]))


# insert a document to the index
def insert_document(document, id_num, index_name):

    # use dictionary comprehension to get wanted values
    new_doc = {k: document.get(k, None) for k in ('entrezgene', 'name', 'symbol', 'taxid')}
    # insert to client
    response = client.index(
        index=index_name,
        body=new_doc,
        id=id_num,
        refresh=True
    )
    logging.debug(response)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    """

    f = open("cdk.json", "r")
    d = json.load(f)

    for hit in d["hits"]:
        id_n = hit["_id"]
        name = "mygene"
        insert_document(hit, id_n, name)
        
    """







