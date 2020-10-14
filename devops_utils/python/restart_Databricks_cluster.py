# ADF API link: https://docs.databricks.com/dev-tools/api/latest/jobs.html
import requests
import datetime
import sys
import json
import time

class MyException(Exception):
    pass

databricksURI = sys.argv[1]
databricksToken = sys.argv[2]
assumedClusterName = sys.argv[3]

get_listClusterEndPoint = "api/2.0/clusters/list"


post_StartClusterEndPoint = "api/2.0/clusters/start"
post_StartClusterBody = {}
post_StartClusterBody["cluster_id"] = "{}"


post_RestartClusterEndPoint = "api/2.0/clusters/restart"
post_RestartClusterBody = {}
post_RestartClusterBody["cluster_id"] = "{}"


header = {}
header["Authorization"]= "Bearer {}".format(databricksToken)


def sendRequests(method,endpoint,headers,body):
    print(body)
    if method =='get':
        requestMade = requests.get(endpoint,headers=headers)
        if not requestMade.status_code == requests.codes.ok:
            print("Status Code is {}".format(requestMade.status_code))
            raise MyException("Request response code is not ok {}".format(requestMade.status_code))
        if requestMade.headers['Content-Type'] == 'application/json':
            recvd_response = requestMade.json()
        else:
            print("taking Text Response")
            recvd_response = requestMade.text
    if method =='post':
        body=json.dumps(body)
        #headers['Content-Type'] = 'application/json'
        requestMade = requests.post(endpoint,headers=headers,data=body)
        if not requestMade.status_code == requests.codes.ok:
            print("Status Code is {}".format(requestMade.status_code))
            print(requestMade.text)
            raise MyException("Request response code is not ok {}".format(requestMade.status_code))
        if requestMade.headers['Content-Type'] == 'application/json':
            recvd_response = requestMade.json()
        else:
            print("taking Text Response")
            recvd_response = requestMade.text
    return recvd_response


def getSparkClusterId(assumedClusterName):
    resp = sendRequests('get',databricksURI + get_listClusterEndPoint ,headers=header ,body='')
    clusterList = resp["clusters"]
    requisiteClusterID = ""
    for _ in clusterList :
        if _["cluster_name"] == assumedClusterName:
            requisiteClusterID = _["cluster_id"]
            break
    if requisiteClusterID == "":
        raise MyException("ClusterID Not Found")
    return requisiteClusterID

def StartCluster(cluster_id):
    post_StartClusterEndPoint["cluster_id"] = "{}".format(cluster_id)
    try:
        resp = sendRequests('post',databricksURI + post_StartClusterEndPoint ,headers=header ,body=post_StartClusterBody)
        return resp
    except Exception as e:
        raise MyException(f"Failed to Start cluster with id {cluster_id}. Exception is {e}")


def RestartCluster(cluster_id):
    post_RestartClusterBody["cluster_id"] = "{}".format(cluster_id)
    try:
        resp = sendRequests('post',databricksURI + post_RestartClusterEndPoint ,headers=header ,body=post_RestartClusterBody)
        return resp
    except Exception as e:
        raise MyException(f"Restart Cluster  with id {cluster_id} Failed. Exception is {e}")


if __name__ == '__main__':
    clusterID = getSparkClusterId(assumedClusterName)
    try:
        resp= RestartCluster(clusterID)
        print(resp)
    except:
        try:
            resp = StartCluster(clusterID)
            print(resp)
        except Exception as e:
            raise MyException(f"Failed to Start a cluster after Restart Failed. Exception is {e}")
