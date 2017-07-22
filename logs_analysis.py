#!/usr/bin/env python

import psycopg2
DBNAME = "news"


def get_most_popular_articles():
    """Return the most popular article of all time"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("select distinct path, count(*) as sum from log where path like\
    '%/article/%' group by path order by sum desc limit 3")
    results = c.fetchall()
    print "\nThe Three Most Popular Articles"
    count = 1
    for row in results:
        article_name = row[0]
        article_name = article_name.replace("/article/", "")
        article_name = article_name.replace("-", " ").title()
        print count, article_name, "--", row[1], "Views"
        count = count + 1
    db.close()


def get_most_popular_authors():
    """Return the most popular authors of all time"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("select authors.name, count(*) as sum from authors join articles\
    on authors.id = articles.author join log on log.path like CONCAT ('%',\
    articles.slug) group by authors.name order by sum desc")
    results = c.fetchall()
    print "\nThe Most Popular Authors of All Time"
    count = 1
    for row in results:
        print count, row[0], "--", row[1], "Views"
        count = count + 1
    db.close()


def report_errors():
    """On Which days did more than 1% of requests lead to erros"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("select time::timestamp::date as day, count(*) as daily_total,\
    sum(case when status like '404 NOT FOUND' then 1 else 0 end) as fail_count\
    from log group by time::timestamp::date")
    print "Days with error larger than 1 percent"
    count = 1
    print "\nDay        ErrorRate"
    results = c.fetchall()
    for row in results:
        error_rate = float(row[2]) / float(row[1])
        if error_rate > .01:
            print count, row[0], error_rate
            count = count + 1
    db.close()

get_most_popular_articles()
get_most_popular_authors()
report_errors()
