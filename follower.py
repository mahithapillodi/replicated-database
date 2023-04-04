#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 14:15:37 2023

@author: miaweaver
"""

class Follower:
    def __init__(self, replica_dispatcher, COMMITTED_DB, PENDING):
        self.db = COMMITTED_DB
        self.pending = PENDING
        self.replica_dispatcher = replica_dispatcher
        
        ##for election...
        self.pending_election = False
        
    def handle_commit(self, msg):
        print(self.pending)
        print("committing to db...")
        ##TODO: write to database pickle file
        self.db[msg["args"][0]] = self.pending[msg["args"][0]]
        del self.pending[msg["args"][0]]
        return

    def handle_appendEntry(self, msg):
        self.pending[msg["args"][0]] = msg["args"][1]
        self.replica_dispatcher.write_msg("appendEntry_pending", msg["src"], None, msg["args"])
        return
    
    def handle_vote_request(self, msg):
        if not self.pending_election:
            self.pending_election = True
            self.replica_dispatcher.write_msg("voted_4_u", msg["src"], None, [])
        return
    
    ## HANDLE HEARTBEAT MESSAGES
    #   update_leader():
    #       if there is a pending election, this means the election is over. The leader for this term has been 
    #           identified. The term's election outcome should be stored in replica state.
    def update_leader(self, msg): ##invoked when heartbeat received after pending election
        self.pending_election = False
        self.replica_dispatcher.last_election[self.replica_dispatcher.term] = msg["src"]
        
    #   handle_heartbeats():
    #       if heartbeat comes during election, set heartbeat timer so next leader downtime can be detected and
    #           handle new identified leader in update_leader()
    #       else reset the heartbeat timer.
    def handle_heartbeats(self, msg):
        if self.pending_election:
            self.replica_dispatcher.set_timer( self.replica_dispatcher.candidate_processes.invoke_election, reset = False )
            self.update_leader(msg["src"])
        else:
            self.replica_dispatcher.set_timer( self.replica_dispatcher.candidate_processes.invoke_election, reset = True )

            
        