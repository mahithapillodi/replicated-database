#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 19:40:06 2023

@author: miaweaver
"""
import os 
import sys
sys.path = [os.getcwd()]

from leader import Leader
from follower import Follower
from candidate import Candidate

from random import choice
import signal
import math
import time

COMMITTED_DB = {"temp" : 0}
PENDING = {}

#### PROTOTYPE MESSAGES TO TEST DISPATCHER CODE LOGIC...
#TEST DESCRIPTION BELOW
m1 = { "type" : "heartbeat",
        "src"  : 0,
        "dst"  : 1,
        "cmd"  : None,
        "args" : [],
        "term" : 0
        }

m2 = { "type" : "db_request",   ##req from client
       "src"  : 4,
       "dst"  : 1,
       "cmd"  : "get",
       "args" : ["temp"],
       "term" : 0
        }

m3 = { "type" : "db_request",
       "src"  : 4,           ##reg from client
       "dst"  : 1,
       "cmd"  : "put",
       "args" : ["temp2", 4],
       "term" : 0} 

vote_msgs = []
for replica in range(7):
    temp_m = { "type" : "voted_4_u",
           "src"  : replica,           ##reg from client
           "dst"  : 1,
           "cmd"  : None,
           "args" : [],
           "term" : 0 
          }
    vote_msgs.append(temp_m)

appendEntry_pending_msgs = []
for replica in range(7):
    temp_m = { "type" : "appendEntry_pending",
           "src"  : replica,           ##reg from client
           "dst"  : 1,
           "cmd"  : None,
           "args" : ["temp2", 4],
           "term" : 0 
          }
    appendEntry_pending_msgs.append(temp_m)

m4 = { "type" : "db_request",   ##req from client
       "src"  : 4,
       "dst"  : 1,
       "cmd"  : "get",
       "args" : ["temp2"],
       "term" : 0
        }

m5 = { "type" : "heartbeat",
       "src"  : 4,
       "dst"  : 1,
       "cmd"  : None,
       "args" : [],
       "term" : 1 }

vote_2_msgs = []
for replica in range(7):
    temp_m = { "type" : "voted_4_u",
           "src"  : replica,           ##reg from client
           "dst"  : 1,
           "cmd"  : None,
           "args" : [],
           "term" : 2
          }
    vote_2_msgs.append(temp_m)

TEST = {}
TEST[0] = vote_msgs + [m2, m3, m4]
TEST[1] = appendEntry_pending_msgs + [m4, m5]
TEST[2] = vote_2_msgs

########################################################################################################################
#pre-test : election invoked; local set to candidate and sends out vote for me requests
#TEST[0] - candidate receives votes from all other replicas & is elected leader
#          receives & services 2 get_requests; receives put request, adds to pending (uncommitted) DB
#          sends appendEntries to replicas
#TEST[1] - candidate receives appendEntry_pending msgs from all clients, meaning all clients have accepted
#          the value. Local commits put request to committed DB, notifies replicas to do the same. Services
#          get_request for the value that was just committed. Receives heartbeat, indicating new leader; local
#          updates to follower...
#TEST[2] - sleep until heartbeat timeout and invokes election upon timeout. Receives votes from all replicas,
#          & updates self to leader.



########################################################################################################################
###DISPATCHER -- should be replicated and ran on all nodes. When ran, dispatcher also initializes candidate,
#                follower, and leader processes; these processes are only available if the replica state matches.
#               
#JOBS -- listens for and sends out RPC. Parses messages and passes them to appropriate processes (based on incoming
#        RPC and replica role). Packages responses into messages, to invoke proper processes on other replicas. Also
#        responsible for maintaining state (# nodes, # nodes for quorum, replica ID, pending DB, committed DB, term,
#        last election results, last entry)
# 
#https://stackoverflow.com/questions/3586106/perform-commands-over-ssh-with-python : code for ssh'ing into machine
#                           & running commands... could use this from host to initialize the dispatcher (which includes
#                           candidate, follower, and leader processes) across all 

##TODO: potentially separate storage of replica state from dispatcher processes
#       received messages currently handled sequentially; should be handled concurrently
#       implement RPC; in the mean time, manually crafting and sending messages in the listen() function, which should
#            be where incoming RPC is detected
#       open, edit, & close pickle files instead of adding to dictionaries
#       figure out how to keep all followers from becoming candidates at once; likely need to use random timer
#           to decide when to invoke election

class Dispatcher:
    def __init__(self, replica_id, leader, nodes):
        self.curr_state = "F" if replica_id != leader else "L"
        self.id = replica_id
        self.replica_ids = [ i for i in range(nodes) if i != self.id ]
        self.quorum = math.floor(nodes/2) + 1 ##some subset of nodes
        self.term = 0
        
        self.leader_processes = Leader(self, COMMITTED_DB, PENDING)
        self.follower_processes = Follower(self, COMMITTED_DB, PENDING)
        self.candidate_processes = Candidate(self, COMMITTED_DB, PENDING)
        
        if leader:
            self.last_election = {self.term : leader}
        else:
            time.sleep( choice(range(0, 2)) ) ##vary the amount of time before declaring self candidate so replicas
                                            ##do not all become candidates at once
            self.last_election = {}
            self.candidate_processes.election(timeout = False)
            
        self.last_entry = {}
        self.event_loop()
        
    def get_leader_id(self):
        return self.last_election[ max(self.last_election.keys()) ]

    ## RECEIVE AND SEND RPC
    # to be intialized later; in mean time, simulating msg passing...
    def listen(self):
        ##check if incoming RPC via pyRPC or socket...?
        ##NOTE: using manually crafted, temporary messages until RPC mechanism is implemented...
        received_msgs = []
        return received_msgs

    def send(self, msg):
        ##send outgoing RPC via pyRPC or socket
        return 
    #''' end receive and send RPC '''

    #''' MESSAGE WRITER - for packaging outgoing rpc'''
    #can request msg be sent to ALL replicas, a specific replica, or to a subset of replicas;
    #depends on the type of the argument "dst"
    #cmd specifies if get or put (only relevant to appendEntries and db_requests)
    #args specify relevant arguments to put into neighbor replica processes
    def write_msg(self, msg_type, dst, cmd, args):
        if dst == "all":
            for rep_id in self.replica_ids:
                temp_msg = { "type" : msg_type,
                             "src"  : self.id, ##replica ID
                             "dst"  : rep_id,
                             "cmd"  : cmd,
                             "args" : args,
                             "term" : self.term ##current term
                           }
                self.send(temp_msg)
                
        elif type(dst) is list:
            for rep_id in dst:
                temp_msg = { "type" : msg_type,
                             "src"  : self.id, ##replica ID
                             "dst"  : rep_id,
                             "cmd"  : cmd,
                             "args" : args,
                             "term" : self.term ##current term

                           }
                self.send(temp_msg)

        elif type(dst) is int and dst in self.replica_ids:
            temp_msg = { "type" : msg_type,
                         "src"  : self.id, ##replica ID
                         "dst"  : dst,
                         "cmd"  : cmd,
                         "args" : args,
                         "term" : self.term, ##current term
                       }
            self.send(temp_msg)
        return
    #''' end msg writer'''        

    # FUNCTION CALLED TO INITIALIZE/RESET HEARTBEAT / ELECTION TIMERS
    # func -- function to invoke if timeout
    #         in case of election timeout, re-invoke election; this case is called from candidate processes
    #         in case of heartbeat timeout,   invoke election; this case is calld from dispatch
    def set_timer(self, func, reset):
        print("Timer started...")
        if reset:
            signal.alarm(0) #stops current alarm
        heartbeat_timeout = choice(range(20, 30))
        signal.signal(signal.SIGALRM, func)
        signal.alarm(heartbeat_timeout)
        return
    #''' end set timer'''
    
    def stop_timer(self):
        print("Timer stopped...")
        signal.alarm(0)
        return

        
    def handle_heartbeats(self, msg):
        if self.curr_state == "C": ##if candidate, this means election failed...
            print("election lost...")
            self.candidate_processes.election_lost()
        elif self.curr_state == "F":
            self.follower_processes.handle_heartbeats()
        elif self.curr_state != "F": ##if for some reason not follower, update to follower...
            print("new leader detected")
            self.curr_state = "F" 
            self.last_election[self.term] = msg["src"]
            self.follower_processes.handle_heartbeats(msg)
        return

    def handle_db_requests(self, msg):
        ##CHECK IF REPLICA IS LEADER OR FOLLOWER
        #db request sent from CLIENTS only... leader sends out appendentires
        #if client request -> leader process IF replica state is leader else return current leader
        if self.curr_state == "L":  #if leader, lookup get, or store put in pending DB and propagate
            self.leader_processes.handle_db_requests(msg)
        elif self.curr_state == "F":  #if follower, return leader ID
            leader_id = self.last_election[ max(self.last_election.keys()) ]
            self.write_msg("leader_id", "all", msg["src"], None, [leader_id])
        return

    def handle_incoming_msgs(self, msg):
        if msg["type"] == "heartbeat":  ##if heartbeat, reset timer...
            self.handle_heartbeats(msg)
        elif msg["type"] == "db_request": ##handle gets & puts
            self.handle_db_requests(msg)
        elif msg["type"] == "voted_4_u":
            if self.curr_state == "C":
                self.candidate_processes.update_votes(msg["src"])
        elif msg["type"] == "appendEntry":
            if self.curr_state == "F":
                self.follower_processes.handle_appendEntry(msg)
        elif msg["type"] == "vote_4_me":
            if self.curr_state == "F":
                self.follower_processes.handle_vote_request(msg)
        elif msg["type"] == "appendEntry_pending":
            if self.curr_state == "L":
                if msg["args"][0] in PENDING.keys():
                    self.leader_processes.handle_appendEntry_pending(msg)
        ##TODO: handle vote_4_me msgs from candidates to followers and 
        #       appendEntry_pending msgs from followers to leaders
        return
        
    
    ##CHECK IF MSG IS VALID; DISCARD IF NOT...
    def check_valid_message(self, msg):
        if msg["dst"] != self.id: ##discard msg if not intended for replica
            return False
        if msg["term"] < self.term: ##discard msg if from previous term
            return False
        if msg["term"] > self.term: ##if observing a higher term
            self.term = msg["term"] #update term
        if msg["type"] == "appendEntry" and msg["src"] != self.get_leader_id() and self.curr_state == "F":
            return False ## only accept appendentries from leader to follower
        return True
    # ''' end msg verifier '''

        

    ## EVENT_LOOP - constantly running, checking for new incoming RPC, 
    ## handling incoming RPC, and sending response RPC... invokes election if necessary via
    ## signal timer process
    def event_loop(self):      
        i = 0
        while i < 3: ## 1 = number of times to check for new RPC
            if i == 2:
                print("sleeping til heartbeat timeout..")
                time.sleep(30)
            received_msgs = TEST[i] #self.listen() ## check if any new rpc messages from clients or replicas
            if received_msgs: ## if yes...
                for msg in received_msgs: ##TODO: HANDLE CONCURRENTLY...
                    if self.check_valid_message(msg): ##check if valid; if yes, handle msg and send appropriate response...
                        self.handle_incoming_msgs(msg) ##updates the msgs to send
                    else:
                        print("INVALID MESSAGE:", msg)
            received_msgs = [] ##re-initialize...
            i+=1
            print("current leader: %d\t current term: %d" % (self.last_election[ max(self.last_election.keys())], self.term) )
            print("elections:", self.last_election)
            print("local pendingDB", PENDING)
            print("commited_db", COMMITTED_DB)
            print("\n")
         ##turn off alarm...
        self.stop_timer()
        return
            
if __name__ == "__main__":
    Dispatcher(1, None, 7)