#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 14:17:46 2023

@author: miaweaver
"""

class Leader:
    def __init__(self, replica_dispatcher, COMMITTED_DB, PENDING):
        self.db = COMMITTED_DB
        self.pending = PENDING
        self.cmd_pending_on = {} #keep track of which entries have been accepted by which replicas

    def data_lookup(self, search_term): ##lookup value in committed DB
        return self.db[search_term]
    
    def pend_msg(self, search_term, value): ##add msg to pending DB
        self.pending[search_term] = value
        return
        
    ##handling database requests from clients...
    def handle_db_reqeuests(self, msg):
        lookup_val = False
        prop_pending = False
        if msg["cmd"] == "get": ##if command is get simply return value OR error
            try:
                lookup_val = self.data_lookup(msg["args"][0])
            except:
                lookup_val = "ERR: no val in database..."
        elif msg["cmd"] == "put": ##if command is put, add to pending and send appendEntries to followers
            self.pend_msg(msg["args"][0], msg["args"][1])
            prop_pending = (msg["args"])
            ##TODO: SEND APPENDENTRIES TO ALL FOLLOWERS
        return self.pending, prop_pending, lookup_val
    
    ##handling when a replica has accepted an appendEntry value
    def handle_appendEntry_pending(self, msg):
        ##keeping track of which append entries have been accepted by which followers with self.cmd_pending_on
        appendEntry_search_term = msg["args"][0] ##search term of appendEntry accepted by follower
        appendEntry_value = msg["args"][0] ##value of appendEntry accepted by follower
        
        if appendEntry_search_term in self.cmd_pending_on.keys(): ##if not already received at least one follower acceptance note
            self.accepted[ appendEntry_search_term ] = [] ##initialize term in pending on dict.
            
        self.accepted[ appendEntry_search_term ].append(msg["dst"]) ##add msg sender as one of the followers that accepted
                                                                    ##the append request...
        if len( self.accepted[ appendEntry_search_term ] ) >= self.replica_dispatch.quorum: #if quorum reached, commit...
            del self.pending[ appendEntry_search_term ]
            self.db[appendEntry_search_term] = appendEntry_value
            ##TODO: SEND MSGS TO ALL REPLICAS NOTIFYING THEM TO COMMIT VAL
        return
             