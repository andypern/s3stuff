#!/usr/bin/env python

import boto3
import botocore

import os
import sys
import getopt
import ConfigParser
import argparse


def getArgs():

    argparser = argparse.ArgumentParser(description = "do basic bucket stuff")

    argparser.add_argument('-e', '--endpoint',
    help = "s3 endpoint, eg: http://storage.googleapis.com:80", required=True)

    argparser.add_argument('-a', '--access_key', dest="access_key",
    help = "s3 access key", required=True)


    argparser.add_argument('-s','--secret', dest="secret_key",
    help = "s3 secret key", required=True)


    argparser.add_argument('-u', '--useSsl',
    dest='useSsl',
    action = 'store_true',
    default=False,
    help = "use ssl..or not default is not")

    argparser.add_argument('-v', '--verbose',
    dest='print_verbose',
    default=False,
    action = 'store_true',
    help = "verbose printing")


    args =  argparser.parse_args()
    return args.endpoint,args.access_key,args.secret_key,args.useSsl,args.print_verbose


def printfail(method,response,print_verbose):
    #
    #this is mainly so we have a consistent way to print shit
    #
    # for later: if there is the '-v' flag set by the user, print more verbose stuff.
    #
    if print_verbose == True:
        print "%s,%s" % (method,response)
    else:
        print "%s,%s" % (method,response['ResponseMetadata']['HTTPStatusCode'])


def printsuccess(method,response,print_verbose):
    #
    #this is mainly so we have a consistent way to print shit
    #
    # for later: if there is the '-v' flag set by the user, print more verbose stuff.
    #
    if print_verbose == True:
        print "%s,%s" % (method,response)
    else:
        try:    
            print "%s,%s" % (method,response['ResponseMetadata']['HTTPStatusCode'])
        except TypeError as e:
            print "%s : 200 , but had TypeError : %s" %(method,e)



def make_session(endpoint,access_key,secret_key,useSsl):

    session = boto3.session.Session()

    
    s3client = session.client('s3',
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            endpoint_url= endpoint,
            #region_name is needed for s3v4. can be pretend.
            region_name="us-west-1",
            use_ssl=useSsl,
            verify=False,
            config=boto3.session.Config(
                #set signature_version to either s3 or s3v4
                #note: if you set to s3v4 you need to set a 'region_name'
                signature_version='s3',
                connect_timeout=60,
                read_timeout=60,
                #for compat with some s3 stores, set to path
                s3={'addressing_style': 'path'})
            )

    return s3client

def list_buckets(s3client,print_verbose):
    method = "list_buckets"

    try:
        response = s3client.list_buckets()
    except botocore.exceptions.ClientError as e:
        printfail(method,e.response,print_verbose)
        sys.exit(0)

    return(response)


def head_bucket(s3client,bucket,print_verbose):

    method = "HEAD Bucket"
    try:
        response = s3client.head_bucket(Bucket=bucket)
        printsuccess(method,response,print_verbose)
        return response
    except botocore.exceptions.ClientError as e:
        printfail(method,e.response,print_verbose)


def list_objects(s3client,bucket,print_verbose):
    #list what's inside
    method = 'list_objects'
    try:
        objList = s3client.list_objects(
            Bucket=bucket,
            MaxKeys=100)
        printsuccess(method,objList,print_verbose)
        return objList
    except botocore.exceptions.ClientError as e:
        printfail(method,e.response,print_verbose)




if __name__ == "__main__":
    
    endpoint,access_key,secret_key,useSsl,print_verbose = getArgs()

    s3client = make_session(endpoint,access_key,secret_key,useSsl) 

    bucket_list = list_buckets(s3client,print_verbose) 

    for bucket in bucket_list['Buckets']:
        print bucket['Name']

    myBucket = bucket_list['Buckets'][0]['Name']


    bucketHead = head_bucket(s3client,myBucket,print_verbose)
    print bucketHead['ResponseMetadata']['HTTPHeaders']
    objList = list_objects(s3client,myBucket,print_verbose)
    print objList 











