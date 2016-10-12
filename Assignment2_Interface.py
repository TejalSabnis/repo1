#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

DATABASE_NAME = 'ddsassignment2'
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'rangeratingspart'
RROBIN_TABLE_PREFIX = 'roundrobinratingspart'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor();
    text_file = open("RangeQueryOut.txt", "w")
    # print("fetching metadata")
    cur.execute("select partitionnum from roundrobinratingsmetadata;");
    partitions = int(cur.fetchone()[0])
    for i in range(0,partitions):
        cur.execute("select userid,movieid,rating from "+RROBIN_TABLE_PREFIX+str(i)+" where rating>=%s and rating<=%s",(ratingMinValue,ratingMaxValue));
        for (userid,movieid,rating) in cur:
            print userid,movieid,rating;
            text_file.write(RROBIN_TABLE_PREFIX+str(i)+","+str(userid)+","+str(movieid)+","+str(rating)+"\n");
    text_file.close();

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor();
    text_file = open("PointQueryOut.txt", "w")
    # print("fetching metadata")
    cur.execute("select partitionnum from roundrobinratingsmetadata;");
    partitions = int(cur.fetchone()[0])
    for i in range(0,partitions):
        cur.execute("select userid,movieid,rating from "+RROBIN_TABLE_PREFIX+str(i)+" where rating=%s",(str(ratingValue),));
        for (userid,movieid,rating) in cur:
            print userid,movieid,rating;
            text_file.write(RROBIN_TABLE_PREFIX+str(i)+","+str(userid)+","+str(movieid)+","+str(rating)+"\n");
    text_file.close();
