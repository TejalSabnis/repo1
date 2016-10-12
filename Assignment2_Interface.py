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
    cur2 = openconnection.cursor();
    text_file = open("RangeQueryOut.txt", "w")
    # fetching from range partitions
    cur.execute("select partitionnum,minrating,maxrating from rangeratingsmetadata;");
    for (partitionnum,minrating,maxrating) in cur:
        if ratingMaxValue>=minrating or ratingMinValue<=maxrating:
            cur2.execute("select userid,movieid,rating from "+RANGE_TABLE_PREFIX+str(partitionnum)+" where rating>=%s and rating<=%s",(ratingMinValue,ratingMaxValue));
            for (userid,movieid,rating) in cur2:
                text_file.write(RANGE_TABLE_PREFIX+str(partitionnum)+","+str(userid)+","+str(movieid)+","+str(rating)+"\n");
    # fetching from round robin partitions
    cur.execute("select partitionnum from roundrobinratingsmetadata;");
    partitions = int(cur.fetchone()[0])
    for i in range(0,partitions):
        cur.execute("select userid,movieid,rating from "+RROBIN_TABLE_PREFIX+str(i)+" where rating>=%s and rating<=%s",(ratingMinValue,ratingMaxValue));
        for (userid,movieid,rating) in cur:
            text_file.write(RROBIN_TABLE_PREFIX+str(i)+","+str(userid)+","+str(movieid)+","+str(rating)+"\n");
    text_file.close();

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor();
    cur2 = openconnection.cursor();
    text_file = open("PointQueryOut.txt", "w")
    # fetching from range partitions
    cur.execute("select partitionnum,minrating,maxrating from rangeratingsmetadata;");
    for (partitionnum,minrating,maxrating) in cur:
        if partitionnum == 0 and ratingValue>=minrating and ratingValue<=maxrating:
            cur2.execute("select userid,movieid,rating from "+RANGE_TABLE_PREFIX+str(partitionnum)+" where rating=%s",(str(ratingValue),));
            for (userid,movieid,rating) in cur2:
                text_file.write(RANGE_TABLE_PREFIX+str(partitionnum)+","+str(userid)+","+str(movieid)+","+str(rating)+"\n");
        else:
            if ratingValue>minrating and ratingValue<=maxrating:
                cur2.execute("select userid,movieid,rating from "+RANGE_TABLE_PREFIX+str(partitionnum)+" where rating=%s",(str(ratingValue),));
                for (userid,movieid,rating) in cur2:
                    text_file.write(RANGE_TABLE_PREFIX+str(partitionnum)+","+str(userid)+","+str(movieid)+","+str(rating)+"\n");
    # fetching from round robin partitions
    cur.execute("select partitionnum from roundrobinratingsmetadata;");
    partitions = int(cur.fetchone()[0])
    for i in range(0,partitions):
        cur.execute("select userid,movieid,rating from "+RROBIN_TABLE_PREFIX+str(i)+" where rating=%s",(str(ratingValue),));
        for (userid,movieid,rating) in cur:
            text_file.write(RROBIN_TABLE_PREFIX+str(i)+","+str(userid)+","+str(movieid)+","+str(rating)+"\n");
    text_file.close();
