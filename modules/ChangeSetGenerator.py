#!/usr/bin/env python3

#######################################################################################
# Change_Set_Generator
#
# Compute the change set needed to bring the local mirror into alignment with the
# remote mirror.
#
# Class takes to packages list from pkgDBPull class and builds a change_set to bring
# the old upto match the new database.
#
# Produces a change_set which has is a list of dicts of the following keys:
#
#   'change'    :   new, update, remove
#   'pkgInfoNew':   package info for the new package comming in.  'None' for a remove package action..
#   'pkgInfoOld':   package info for the old package getting removed. 'None' for a new package action.
#
#   pkginfo is a dict with the following keys ( see: PkgDBPuller.py )
#       pkgName = the name of the package
#       pkgArch = Architecture the package support (amd64 multi-arch arm etc..)
#       pkgVer  = Version number for this package
#       pkgFile = Mirror File Path / Name to the .deb file
#       pkgHash = SHA256 hash finger print for the .deb file (consistency checking)
#
########################################################################################

class ChangeSetGenerator:
    """ compute the change set for 2 pkg_lists """
    def __init__(self, pkg_list_new, pkg_list_current ):
        self.pkg_list_new = pkg_list_new      # remote
        self.pkg_list_old = pkg_list_current  # local
        self.change_set = []
        self.pkg_index_new = {}
        self.pkg_index_old = {}

    """ (INTERNAL) Generate a index hash to
        speed up searches.  The pkg_lists should be 
        in Alphabetical order.  Generate a index
        for the start of each new alaphbet in the list.
        This information is stored in a dict of { "alpha" : "list index" } """
    def gen_dataset_index( self, pkg_list ):
        pkg_index = { 'a':0 }
        char_last = "a"
        index = 0
        for entry in pkg_list:
            pkg_name = entry['pkgName']
            if ( pkg_name[0] != char_last ):
                # found next char index in list, update last
                char_last = pkg_name[0]
                # add to index_db
                pkg_index[char_last] = index

            # next entry
            index = index + 1
        
        # done generating index for database
        return pkg_index

    """ (INTERNAL) fine the package "pkg_name" in the pkg_list
        using the pkg_list_index that was generated to speed up
        searches. """
    def find_package_in_list( self, pkg_name, pkg_list, pkg_list_index ):
        # dumb case if pkg_list is an empty set. (initial repo setup might do this..)
        if ( len(pkg_list) < 1 ):
            return -1 

        # find index to start linear search
        index = pkg_list_index[pkg_name[0]]
        search_active = 1
        while (search_active):
            # does the current index give us a pkg_name match
            if ( pkg_list[index]['pkgName'] == pkg_name ):
                # found it
                search_active = 0;
                return index

            # check to see have not run pass the search section
            # verify that first char of current search index
            # does match the first char of the package we are searching for
            if ( pkg_list[index]['pkgName'][0] != pkg_name[0] ):
                # first chars from search index pkgName does not match
                # the frist char of the package we are looking for..
                # entry must not be here.. (we assume alphabetical ordering in list)
                return -1
            
            # Idiot check to make sure we don't overrun the end of the list
            if (index > len(pkg_list) ):
                # got here, item dose not exist in the list
                return -1
            
            # search next index
            index = index + 1

    """ (INTERNAL) compute change set between to pkg_lists """
    def compute_change_set(self):
        # entry for each change found
        change_set_entry = {}

        # compute search index for each package list.
        self.pkg_index_old = self.gen_dataset_index( self.pkg_list_old )
        self.pkg_index_new = self.gen_dataset_index( self.pkg_list_new )

        # look for new packages or updated packages in new vs old lists
        for new_pkg_entry in self.pkg_list_new:
            # see if we can find a matching package name in the old list
            # print("evaluating package: " + str(new_pkg_entry['pkgName']) + " for changes..")
            i = self.find_package_in_list( new_pkg_entry['pkgName'], self.pkg_list_old, self.pkg_index_old )

            # if search does not return a hit, this package 
            # is in the new list and not the old one, thus it is a new package
            if ( i < 0 ):
                # debug print
                # print("     new package " + str(new_pkg_entry['pkgName']) + " found.." )
                change_set_entry['change'] = "new"
                change_set_entry['pkgInfoNew'] = new_pkg_entry
                change_set_entry['pkgInfoOld'] = None
                change_set_entry['state'] = "queued"
                self.change_set.append( change_set_entry )
            else:
                # package exists in old database as well as new database
                # is the versions any different
                if ( new_pkg_entry['pkgVer'] != self.pkg_list_old[i]['pkgVer'] ):
                    # print("     update package " + str(new_pkg_entry['pkgName']) + " discovered..")
                    # version change detected, mark as an updated package
                    change_set_entry['change'] = "upgrade"
                    # need to know details about the new package (filename, sha256 hash etc.. )
                    change_set_entry['pkgInfoNew'] = new_pkg_entry 
                    # need to know details about the old package (filename, sha256 hash etc.. )
                    change_set_entry['pkgInfoOld'] = self.pkg_list_old[i]
                    # define this change as queued
                    self.change_set.append( change_set_entry )

            change_set_entry = {}

        # end new/update search

        # Now to look for packages that have been deprecated. (removed from repository)
        for old_pkg_entry in self.pkg_list_old:
            # search for each package from old list in new list to see if it exists.
            i = self.find_package_in_list( old_pkg_entry['pkgName'], self.pkg_list_new, self.pkg_index_new )
            # only care if there is a matching package name or not..
            if ( i < 0 ):
                # means package name we are looking for does not exists in the new list. (was removed)
                # print("     remove package " + str(old_pkg_entry['pkgName']) )
                change_set_entry['change'] = "remove"
                change_set_entry['pkgInfoNew'] = None
                change_set_entry['pkgInfoOld'] = old_pkg_entry
                self.change_set.append( change_set_entry )

            change_set_entry = {}

        #end old search
    # end compute_change_set

# end change_set_Generator

