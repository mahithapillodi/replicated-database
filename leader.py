#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 14:17:46 2023

@author: miaweaver
"""

class Leader:
    def __init__(self, replica_dispatcher, COMMITTED_DB, PENDING):
        self.replica_dispatcher = replica_dispatcher
        self.db = COMMITTED_DB
        self.pending = PENDING
        self.cmd_pending_on = {} #keep track of which entries have been accepted by which replicas

    ## DATA LOOKUP
    #   For servicing get_requests
    #TODO: read from pickle file...
    def data_lookup(self, search_term): ##lookup value in committed DB
        return self.db[search_term]
    
    ## PEND MSG
    #   Add data value to transient store for uncommitted data
    def pend_msg(self, search_term, value): ##add msg to pending DB
        self.pending[search_term] = value
        return
    
    ## SERVICE CLIENT REQUESTS
    #   Responds to get requests with value if it exists, else responds with error message.
    #   Responds to put request by adding the value to a temporary data store for uncommitted values
    #       and sending appendEntries to replicas.
    def handle_db_requests(self, msg):
        lookup_val = False
        prop_pending = False
        if msg["cmd"] == "get": ##if command is get simply return value OR error
            try:
                lookup_val = self.data_lookup(msg["args"][0])
            except:
                lookup_val = "ERR: no val in database..."
            self.replica_dispatcher.write_msg("db_request_resp", msg["src"], None, [lookup_val])
            print("returned get request", msg["args"][0], lookup_val)
        elif msg["cmd"] == "put": ##if command is put, add to pending and send appendEntries to followers
            self.pend_msg(msg["args"][0], msg["args"][1])
            self.replica_dispatcher.write_msg("appendEntry", "all", "put", msg["args"]) ##prop to all replicas
        return self.pending, prop_pending, lookup_val
    
    ## HANDLE ACCEPTED APPEND ENTRIES
    #   handling when a replica has accepted an appendEntry value
    #       keep track of number of votes for pending term vs. number to quorum
    #       when quorum reached, commit to DB
    #TODO: write commits to pickle file...
    def handle_appendEntry_pending(self, msg):
        ##keeping track of which append entries have been accepted by which followers with self.cmd_pending_on
        appendEntry_search_term = msg["args"][0] ##search term of appendEntry accepted by follower
        appendEntry_value = msg["args"][1] ##value of appendEntry accepted by follower
        
        if  (appendEntry_search_term not in self.cmd_pending_on.keys() and 
            self.pending[appendEntry_search_term] == appendEntry_value): ##if not already received at least one follower acceptance note
            self.cmd_pending_on[ appendEntry_search_term ] = [] ##initialize term in pending on dict.
            
        self.cmd_pending_on[ appendEntry_search_term ].append(msg["dst"]) ##add msg sender as one of the followers that accepted
                                                                    ##the append request...
        if len( self.cmd_pending_on[ appendEntry_search_term ] ) >= self.replica_dispatcher.quorum: #if quorum reached, commit...
            print(msg["args"], "being committed...")
            self.db[appendEntry_search_term] = appendEntry_value
            self.replica_dispatcher.write_msg("commit_appendEntry", "all", None, msg["args"])
            del self.pending[ appendEntry_search_term ]
        return
             