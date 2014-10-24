#!/usr/bin/env python3

################################################################################################
# Mirror_Magic Multithreaded Downloader
################################################################################################
# Downloader.py
#
# This module is used to download files from a remote server using a multithreaded connections
# It's main purpose is to fetch all the remote packages that need to be downloaded and stored
# on our local mirror based on changes detected between the local mirror and the remote mirror.
#
# This object takes a list of job dicts in the following format
#
#   job dict:
#   "src"  -- src file to download (url: http://blah.com/stuff.deb )
#   "dst"  -- dst filepath/name to save downloaded file too. (ex: /opt/mirror_magic/stuff.deb )
#   "hash" -- SHA256 hash string in hex to validate downloaded file with.
#   "complete" -- True or False
#   "trys" -- a number of time we have tried to fetch this job 0 -- not tried yet..
#
#################################################################################################

import concurrent.futures
from random import randint
import urllib
import hashlib
import time


""" fetches on file per call 
    job_data should a dict with "src", "dst", and "hash" keys defined 
    called as part of a ThreadPoolExecutor """
def fetcher(job):
    # update try counter for this job
    job['trys'] = job['trys']+1
 
    if ( job_selftest == 1 ):
        print("Processing job: "+str(job))
        time.sleep(randint(1,3))
        if ( randint(0,1) > 0.5):
            # simulate a failed dowload
            print("Download: "+job['src']+" -> "+job['dst']+" hash: "+job['hash']+ "   Failed!" )
            job['complete'] = False
        else:
            print("Download: "+job['src']+" -> "+job['dst']+" hash: "+job['hash']+ "   Success!" )
            job['complete'] = True
    else:
        # this is real
        print("Fetching: "+str(job['src']))
        # use urllib to download file and save to local filesystem
        try:
            sha_gen = hashlib.sha256()
            response = urlopen( job['src'] )
            file_data = response.read()
            sha_gen.update(file_data)
            
            if ( sha_gen.hexdigest() == job['hash'] ):
                # we have good data, save to dst
                fh = open( job['dst'], "wb" )
                fh.write( file_data )
                job['complete'] = True
            else:
                # bad SHA Hash
                job['complete'] = False

        except urllib.error.URLError as e:
            print("Download error: "+str(e.reason) )
            job['complete'] = False
 
        except urllib.error.HTTPError as e:
            print("Download http error #"+str(e.code) )
            job['complete'] = False
 
        except IOError as e:
            print("Download IO Error: "+str(e) )
            job['complete'] = False

    # return job status to query
    return job

 
""" threaded downloader
    uses a thread pool executor to generate futures.
    Futures are thread tasks that get executed by the thead pool.
    job_list is a list of dicts with the following keys:
        "src"  -- src file to download (url: http://blah.com/stuff.deb )
        "dst"  -- dst filepath/name to save downloaded file too. (ex: /opt/mirror_magic/stuff.deb )
        "hash" -- SHA256 hash string in hex to validate downloaded file with.
        "complete" -- True (file download successfull), False ( Need a refetch )
    """  
def threaded_downloader( job_list, thread_count ):
    failed_jobs_list = []
    tpe = concurrent.futures.ThreadPoolExecutor(thread_count)
    future_to_job = ( tpe.submit(fetcher, job) for job in job_list )
    for future in concurrent.futures.as_completed(future_to_job):
        if (future.result()['complete'] == False ):
            failed_jobs_list.append( future.result() )         

    # wait for jobs to finish
    tpe.shutdown(wait=True)

    # return list of failed jobs
    return failed_jobs_list
    

job_selftest = 0     
    
# Test Stuff    
if __name__ == "__main__":
    # Excuting job self_set
    job_selftest = 1

    # Test Job List
    job_list = [ { 'src' : "abcd", 'dst' : "sdrs", 'hash' : "240abcd349393", 'complete' : False, 'trys' : 0 },
                 { 'src' : "aibm", 'dst' : "svds", 'hash' : "3453cd3493938", 'complete' : False, 'trys' : 0 },
                 { 'src' : "dmkl", 'dst' : "sirm", 'hash' : "7685833cb93e1", 'complete' : False, 'trys' : 0 },
                 { 'src' : "mame", 'dst' : "amre", 'hash' : "290bc392e92f3", 'complete' : False, 'trys' : 0 },
                 { 'src' : "mtyt", 'dst' : "ghgy", 'hash' : "290bc392e92f3", 'complete' : False, 'trys' : 0 },
                 { 'src' : "mghy", 'dst' : "nkjg", 'hash' : "290bc392e92f3", 'complete' : False, 'trys' : 0 },
                 { 'src' : "vdfr", 'dst' : "amre", 'hash' : "290bc392e92f3", 'complete' : False, 'trys' : 0 },
                 { 'src' : "tdkk", 'dst' : "amre", 'hash' : "290bc392e92f3", 'complete' : False, 'trys' : 0 },
                 { 'src' : "ptol", 'dst' : "vmwd", 'hash' : "290bc392e92f3", 'complete' : False, 'trys' : 0 } ]

    failed_jobs_list = threaded_downloader( job_list, 4 )
    while (failed_jobs_list):
        print("Failed jobs after last attempt: ")
        print( str(failed_jobs_list) )
        # retry failed jobs
        failed_jobs_list = threaded_downloader( failed_jobs_list, 4 )


