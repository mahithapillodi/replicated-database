#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 14:28:46 2023

@author: miaweaver
"""

class Candidate:
    def __init__(self, replica_dispatcher, COMMITTED_DB, PENDING):
        self.votes = []
        self.pending_election = False ##keeps track of if election occurring
        self.replica_dispatcher = replica_dispatcher
        
        
    ## HANDLE VOTES:
    #       invoked when dispatcher receives a 'voted_4_u' message
    #       update the number of votes stored by replica's candidate process
    #       if quorum reached, update self to leader, stop election.
    def update_votes(self, voter):
        if self.replica_dispatcher.curr_state == "C": ##if candidate and receives votes, update list of voters for candidate...
            if voter not in self.votes:
                self.votes.append(voter)
            print("CURRENT VOTES:", self.votes)
        if len(self.votes) >= self.replica_dispatcher.quorum: ##if received a quorum of votes
            print("quorum reached. replica now leader.")
            self.pending_election = False #stop the election
            self.replica_dispatcher.curr_state = "L" #update state of dispatcher
            self.votes = [] ##re-initialize votes
            self.replica_dispatcher.last_election[self.replica_dispatcher.term] = self.replica_dispatcher.id
            self.replica_dispatcher.stop_timer()
        return
            

    ## ELECTION HANDLER FUNCTIONS:
    #  election_lost():
    #       called in case of heartbeat timeout, or upon rep db initialization, if
    #       db initialized with no leader
    def election_lost(self): ##invoked when dispatcher in candidate state and receives heartbeat
        self.pending_election = False #stop the election
        self.replica_dispatcher.curr_state = "F" #update state of dispatcher back to follower
        self.votes = [] ##re-initialize votes for next election
        self.replica_dispatcher.set_timer( self.replica_dispatcher.candidate_processes.invoke_election, reset = False) ##initialize heartbeat timer
        return
    #  election_timeout():
    #       called when election times out; simply re-invokes election process    
    def election_timeout(self, signum, frame):
        self.replica_dispatcher.term += 1
        print("Election timeout... invoking re-election on replica %d" % self.replica_dispatcher.id )
        self.election() ##commented out for now so we do not enter infinite elections

    #  election():
    #       prints message if initializing DB, otherwise called from timeout
    #       updates dispatcher state to candidate, increments term, sets election timer
    #       and sends "vote_4_me" requests to all clients
    def election(self, timeout):
        if not timeout:
            print("Initializing leader...")
        else:
            self.replica_dispatcher.term += 1
        self.pending_election = True
        self.replica_dispatcher.curr_state = "C"
        self.replica_dispatcher.set_timer(self.election_timeout, reset = True)
        self.replica_dispatcher.write_msg("vote_4_me", "all", None, None) ##write vote requests and send to all clients
        
    #  invoke_election():
    #       heartbeat timeout occurred; print notification and start election process
    def invoke_election(self, signum, frame): ##invoke election from heartbeat timeout (invoked from dispatcher)
        if not self.replica_dispatcher.curr_state == "L": #if heartbeat timeout on non-leader replica...
            print( "Heartbeat timed out... invoking election on replica %d" %  self.replica_dispatcher.id )
            self.election(timeout = True)
        return
    ## ''' end election handler '''