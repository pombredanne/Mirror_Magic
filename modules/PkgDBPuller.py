#!/usr/bin/env python3
#############################################################################################################
# Mirror Magic PkgDBPuller
#############################################################################################################
# PkgDBPuller
# Pulls the packages database for the dist/section/arch specified from the remote mirror specified.
# Saves a copy of the donwloaded database file into a local incomming directory.
# ( ${mirror_local_root}/incomming/${dists}/${section}/binary-${arch}/Packages.[ bz2 | xz ] )
#
# The PkgDBPuller will decompress the database file and parse out the package information
# This information is stored into a list of dicts with the following keys
#
#   PkgDataBase List Entry available Keys
#       pkgName = the name of the package
#       pkgArch = Archtecture the package support (amd64 multiarch arm etc..)
#       pkgVer  = Version number for this package
#       pkgFile = Mirror File Path / Name to the .deb file
#       pkgHash = SHA256 hash finger print for the .deb file (consistancy checking)
#
###############################################################################################################

import os
import errno
import re
import urllib.request import urlopen
import bz2
import lzma
import hashlib


class PkgDBPuller:
    """ when this class is initalized, it needs to know 
        the url to the remote mirror root and
        the path to the local mirror root. """
    def __init__(self, remote_mirror_root_url, local_mirror_root_path):
        self.rmr_url = remote_mirror_root_url # url to remote mirror root: ex: http://archives.ubuntu.com/ubuntu/
        self.lmr_path = local_mirror_root_path # local filesystem path for mirror root: ex: /opt/mirror_magic/ubuntu/
        
    """ Fetch and Parse Pkg Database for the vendor/dist/arch/section we want from the local mirror """
    def fetch_and_parse_local(self, vendor="ubuntu", dist="trusty", arch="amd64", section="main")
        # Connect to mirror and pull packages.bz2 (ubuntu/default) or packages.xz (debian)
        if (vendor == "debian"):
            url = "file://"+self.lmr_path+"/dists/"+dist+"/"+section+"/binary-"+arch+"/Packages.xz"
        else:
            # pull bz2 file (ubuntu and others)
            url = "file://"+self.lmr_path+"/dists/"+dist+"/"+section+"/binary-"+arch+"/Packages.bz2"

        # indicate what file we are downloading
        print("fetching: "+str(url))

        # download packages files
        try:
            reponse = urlopen( url )
        except urllib.error.URLError as e:
            print("(Skip) Error for URL \""+str(url)+"\" : "+str(e.reason) )
            # abort fetch
            return []
        except urllib.error.HTTPError as e:
            print("(Skip) HTTP Error for URL \""+str(url)+"\" : error "+str(e.code) )
            # abort fetch
            return []
        except IOError as e:
            print("(Skip) File IO Error for URL \n"+str(url)+"\" : error "+str(e)

        # read response from filesystemr (should be file data)
        pkgData_Compressed = response.read()

        # Decompress the data
        if ( vendor == "debian" ):
            pkgData_bytes = lzma.decompress( PkgData_Compressed )
        else:
            pkgData_bytes = bz2.decompress( PkgData_Compressed )

        # Decompression leaves us with a array of bytes
        # file data is UTF-8, and we want lines of text to parse
        line = ""
        pkgData = [ "" ] # list of lines
        for c in pkgData_bytes.decode('UTF-8'):
            line = line + c
            if c == "\n":
                # add line to list of lines
                pkgData.append(line)
                # start new line
                line = ""

        return self.parsePkgData( pkgData )

    """ Fetch and parse Pkg Database for the vendor/dist/arch/section we want from a remote mirror """
    def fetch_and_parse_remote(self, vendor="ubuntu", dist="trusty", arch="amd64", section="main")
        # Connect to mirror and pull packages.bz2 (ubuntu/default) or packages.xz (debian)
        if (vendor == "debian"):
            url = self.rmr_url+"/dists/"+dist+"/"+section+"/binary-"+arch+"/Packages.xz"
            lfp = self.lmr_path+"/dists/"+dist+"/"+section+"/binary-"+arch+"/"
            lfn = "Packages.xz"
        else:
            # pull bz2 file (ubuntu and others)
            url = self.rmr_url+"/dists/"+dist+"/"+section+"/binary-"+arch+"/Packages.bz2"
            lfp = self.lmr_path+"/dists/"+dist+"/"+section+"/binary-"+arch+"/"
            lfn = "Packages.bz2"

        # indicate what file we are downloading
        print("fetching: "+str(url))

        # download packages files
        try:
            reponse = urlopen( url )
        except urllib.error.URLError as e:
            print("(Skip) Error for URL \""+str(url)+"\" : "+str(e.reason) )
            # abort fetch
            return []
        except urllib.error.HTTPError as e:
            print("(Skip) HTTP Error for URL \""+str(url)+"\n : error "+str(e.code) )
            # abort fetch
            return []

        # read response from webserver (should be file data)
        pkgData_Compressed = response.read()

        # Save this data to a file on the local mirror tmp location
        # This will become out new packages file for the local mirror when we are done
        # pulling all the new and updated packages.
        # store to ${mirror_local_root}/incomming/${dists}/${section}/binary-${arch}/Packages.(bz2/xz)
        # step one, make the directory path
        os.mkdirs( lfp, mode=0o777, exist_ok=True)
        try:
            fh = open( lfp+lfn, "wb" )
            fh.write( pkgData_Compressed )
        except IOError as e:
            print("(Skip) File IO Error when saving pkgfile: "+str(lfp+lfn)+" => "+str(e) )
            return []

        # Decompress the data
        if ( vendor == "debian" ):
            pkgData_bytes = lzma.decompress( PkgData_Compressed )
        else:
            pkgData_bytes = bz2.decompress( PkgData_Compressed )

        # Decompression leaves us with a array of bytes
        # file data is UTF-8, and we want lines of text to parse
        line = ""
        pkgData = [ "" ] # list of lines
        for c in pkgData_bytes.decode('UTF-8'):
            line = line + c
            if c == "\n":
                # add line to list of lines
                pkgData.append(line)
                # start new line
                line = ""

        # parse packages database return list of package dicts
        return self.parsePkgData( pkgData )

    """ (Internal) Read package data, build a dict for each package.
        Place each dict into a list.
        Debian and Ubuntu both use the same package database format """
    def parsePkgData(self, PkgData ):
        pkg_db_entry = {}
        pkg_list = []

        # build compiled regular expressions for use in parser
        pkgName_re = re.compile("Package: (.+)")
        pkgArch_re = re.compile("Architecture: (.+)")
        pkgVer_re  = re.compile("Version: (.+)")
        pkgFile_re = re.compile("Filename: (.+)")
        pkgHash_re = re.compile("SHA256: (.+)")

                
        # read file database, line by line
        for line in PkgData:
            line = line.rstrip()  # remove trailing whitespace and new lines from line.

            # look for data matches against regular expressions
            m = pkgName_re.match( line )
            if m:
                pkg_db_entry['pkgName'] = m.group(1)

            m = pkgArch_re.match( line )
            if m:
                pkg_db_entry['pkgArch'] = m.group(1)

            m = pkgVer_re.match(line)
            if m:
                pkg_db_entry['pkgVer'] = m.group(1)

            m = pkgFile_re.match(line)
            if m:
                pkg_db_entry['pkgFile'] = m.group(1)

            m = pkgHash_re.match(line)
            if m:
                pkg_db_entry['pkgHash'] = m.group(1)

            if ( line == "" ):
                # end of record found, push back db_entry to database
                # for debugging purposes
                # print("adding pkg: ", db_entry )
                if not pkg_db_entry:  # entry is empty..
                    # nothing to add.. false end of record detected..
                    pass
                else:
                    # add entry to dict
                    pkg_list.append( pkg_db_entry );

                pkg_db_entry = {} # reset dict, ready for next new entry

            # end if
        
        return pkg_list
    # end parsePkgData

        
        
        

        

         
        
        



